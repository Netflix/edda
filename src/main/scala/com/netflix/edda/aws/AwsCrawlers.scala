package com.netflix.edda.aws

import com.netflix.edda.StateMachine
import com.netflix.edda.Crawler
import com.netflix.edda.CrawlerState
import com.netflix.edda.Observable
import com.netflix.edda.Record

import com.netflix.edda.BeanMapperComponent
import com.netflix.edda.CrawlerComponent

import org.joda.time.DateTime

import com.amazonaws.services.ec2.model.DescribeAddressesRequest
import com.amazonaws.services.ec2.model.DescribeImagesRequest
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest
import com.amazonaws.services.ec2.model.DescribeTagsRequest
import com.amazonaws.services.ec2.model.DescribeVolumesRequest

import com.amazonaws.services.s3.model.ListBucketsRequest

import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsRequest

import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest
import com.amazonaws.services.elasticloadbalancing.model.DescribeInstanceHealthRequest


import collection.JavaConversions._
import scala.actors.Futures.{future, awaitAll}
import scala.actors.Actor

trait AwsCrawlerBuilder extends AwsClientComponent with BeanMapperComponent {
    val builderClient = awsClient
    val builderMapper = beanMapper

     trait Builder extends AwsClientComponent with BeanMapperComponent {
         val awsClient = builderClient
         val beanMapper = builderMapper
     }

    def build(): Map[String,Crawler] = {
        val aws = Map(
            "aws.addresses" -> new AddressCrawler with Builder,
            "aws.autoScalingGroups" -> new AutoScalingGroupCrawler with Builder,
            "aws.images" -> new ImageCrawler with Builder,
            "aws.loadBalancers" -> new LoadBalancerCrawler with Builder,
            "aws.launchConfigurations" -> new LaunchConfigurationCrawler with Builder,
            "aws.instances" -> new ReservationCrawler with Builder,
            "aws.securityGroups" -> new SecurityGroupCrawler with Builder,
            "aws.snapshots" -> new SnapshotCrawler with Builder,
            "aws.tags" -> new TagCrawler with Builder,
            "aws.volumes" -> new VolumeCrawler with Builder,
            "aws.buckets" -> new BucketCrawler with Builder
        )

        Map(
            "view.loadBalancerInstances" -> new InstanceHealthCrawler with Builder {
                val crawler = aws("aws.loadBalancers")
            },
            "view.instances" -> new InstanceCrawler with Builder {
                val crawler = aws("aws.instances")
            }
        ) ++ aws
    }
}

trait AddressCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeAddressesRequest
    override def doCrawl =
        awsClient.ec2.describeAddresses(request).getAddresses.toList.map(
            item => Record(item.getPublicIp, beanMapper(item))
        )
}

trait AutoScalingGroupCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeAutoScalingGroupsRequest
    override def doCrawl = {
        val it = new AwsIterator() {
            def next = {
                val response = awsClient.asg.describeAutoScalingGroups(request.withNextToken(this.nextToken))
                this.nextToken = response.getNextToken
                response.getAutoScalingGroups.toList.map(
                    item => Record(item.getAutoScalingGroupName, new DateTime(item.getCreatedTime), beanMapper(item))
                )
            }
        }
        it.flatten.toList
    }
}

trait ImageCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeImagesRequest
    override def doCrawl =
        awsClient.ec2.describeImages(request).getImages.toList.map(
            item => Record(item.getImageId, beanMapper(item))
        )
}

trait LoadBalancerCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeLoadBalancersRequest
    override def doCrawl = awsClient.elb.describeLoadBalancers(request).getLoadBalancerDescriptions.toList.map(
        item => Record(item.getLoadBalancerName, new DateTime(item.getCreatedTime), beanMapper(item))
    )
}

case class InstanceHealthCrawlerState(elbRecords: List[Record] = List[Record]())

object InstanceHealthCrawler extends StateMachine.LocalState[InstanceHealthCrawlerState]

trait InstanceHealthCrawler extends Crawler with CrawlerComponent with AwsClientComponent with BeanMapperComponent {
    import InstanceHealthCrawler._
    override def crawl() = crawler.crawl
    override def doCrawl = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceHealthCrawler")
    def doCrawl(elbRecords: List[Record]): List[Record] = {
        val tasks = elbRecords.map(elb => future {
            awsClient.elb.describeInstanceHealth(new DescribeInstanceHealthRequest(elb.id)).getInstanceStates.toList.map(
                instances => elb.copy(data=Map("name" -> elb, "instances" -> instances))
            )
        })
        awaitAll(300000L, tasks:_*) match { 
            case x: List[Some[Record]] => x.map( _.get )
        }
    }

    protected override
    def initState = addInitialState(super.initState, newLocalState(InstanceHealthCrawlerState()))
    
    protected override
    val init = crawler.addObserver(this)

    protected
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawler.CrawlResult(elbRecords),state) => {
            // this is blocking so we dont crawl in parallel
            // TODO return state if elbRecords == state.elbRecords && minCycle not reached
            val newRecords = doCrawl(elbRecords)
            Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
            setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), InstanceHealthCrawlerState(elbRecords))
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}

trait LaunchConfigurationCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeLaunchConfigurationsRequest
    override def doCrawl = {
        val it = new AwsIterator() {
            def next = {
                val response = awsClient.asg.describeLaunchConfigurations.withNextToken(this.nextToken)
                this.nextToken = response.getNextToken
                response.getLaunchConfigurations.toList.map( 
                    item => Record(item.getLaunchConfigurationName, new DateTime(item.getCreatedTime), beanMapper(item))
                )
            }
        }
        it.flatten.toList
    }
}

trait ReservationCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeInstancesRequest
    override def doCrawl = awsClient.ec2.describeInstances(request).getReservations.toList.map(
        item => Record(item.getReservationId, beanMapper(item))
    )
}

case class InstanceCrawlerState(reservationRecords: List[Record] = List[Record]())

object InstanceCrawler extends StateMachine.LocalState[InstanceCrawlerState]

trait InstanceCrawler extends Crawler with CrawlerComponent with AwsClientComponent with BeanMapperComponent {
    import InstanceCrawler._
    override def crawl() = crawler.crawl
    override def doCrawl = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceCrawler")
    def doCrawl(resRecords: List[Record]): List[Record] = {
        resRecords.flatMap(rec => {
            rec.data.asInstanceOf[Map[String,Any]].get("instances") match {
                case instances: List[Map[String,Any]] => instances.map(
                    (inst: Map[String,Any]) => rec.copy(
                        id=inst("instanceId").asInstanceOf[String],
                        data=inst,
                        ctime=inst("launchTime").asInstanceOf[DateTime]
                    )
                )
                case other => throw new java.lang.RuntimeException("failed to crawl instances from reservation, got: " + other)
            }
        })
    }

    protected override
    def initState = addInitialState(super.initState, newLocalState(InstanceCrawlerState()))
    
    protected override
    val init = crawler.addObserver(this)

    protected
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawler.CrawlResult(reservations),state) => {
            // this is blocking so we dont crawl in parallel
            // TODO return state if elbRecords == state.elbRecords && minCycle not reached
            val newRecords = doCrawl(reservations)
            Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
            setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), InstanceCrawlerState(reservations))
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}


trait SecurityGroupCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeSecurityGroupsRequest
    override def doCrawl = awsClient.ec2.describeSecurityGroups(request).getSecurityGroups.toList.map(
        item => Record(item.getGroupId, beanMapper(item))
    )
}

trait SnapshotCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeSnapshotsRequest
    override def doCrawl = awsClient.ec2.describeSnapshots(request).getSnapshots.toList.map(
        item => Record(item.getSnapshotId, new DateTime(item.getStartTime), beanMapper(item))
    )
}

trait TagCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeTagsRequest
    override def doCrawl = awsClient.ec2.describeTags(request).getTags.toList.map(
        item => Record(item.getKey() + "|" + item.getResourceType() + "|" + item.getResourceId(), beanMapper(item))
    )
}

trait VolumeCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new DescribeVolumesRequest
    override def doCrawl = awsClient.ec2.describeVolumes(request).getVolumes.toList.map(
        item => Record(item.getVolumeId, new DateTime(item.getCreateTime), beanMapper(item))
    )
}

trait BucketCrawler extends Crawler with AwsClientComponent with BeanMapperComponent {
    val request = new ListBucketsRequest
    override def doCrawl = awsClient.s3.listBuckets(request).toList.map(
        item => Record(item.getName, new DateTime(item.getCreationDate), beanMapper(item))
    )
}
