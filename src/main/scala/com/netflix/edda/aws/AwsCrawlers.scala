package com.netflix.edda.aws

import com.netflix.edda.StateMachine
import com.netflix.edda.Crawler
import com.netflix.edda.CrawlerState
import com.netflix.edda.Observable
import com.netflix.edda.Record

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

class AddressesCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeAddressesRequest
    override def doCrawl =
        client.ec2.describeAddresses(request).getAddresses.toList.map(
            item => Record.fromBean(item.getPublicIp, item)
        )
}

class AutoScalingGroupCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeAutoScalingGroupsRequest
    override def doCrawl = {
        val it = new AwsIterator() {
            def next = {
                val response = client.asg.describeAutoScalingGroups(request.withNextToken(this.nextToken))
                this.nextToken = response.getNextToken
                response.getAutoScalingGroups.toList.map(
                    item => Record.fromBean(item.getAutoScalingGroupName, item, new DateTime(item.getCreatedTime))
                )
            }
        }
        it.flatten.toList
    }
}

class ImagesCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeImagesRequest
    override def doCrawl =
        client.ec2.describeImages(request).getImages.toList.map(
            item => Record.fromBean(item.getImageId, item)
        )
}

class LoadBalancerCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeLoadBalancersRequest
    override def doCrawl = client.elb.describeLoadBalancers(request).getLoadBalancerDescriptions.toList.map(
        item => Record.fromBean(item.getLoadBalancerName, item, new DateTime(item.getCreatedTime))
    )
}

case class InstanceHealthCrawlerState(elbRecords: List[Record] = List[Record]())

object InstanceHealthCrawler extends StateMachine.LocalState[InstanceHealthCrawlerState]

class InstanceHealthCrawler(client: AwsClient, elbCrawler: LoadBalancerCrawler) extends Crawler {
    import InstanceHealthCrawler._
    override def crawl() = elbCrawler.crawl
    override def doCrawl = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceHealthCrawler")
    def doCrawl(elbRecords: List[Record]): List[Record] = {
        val tasks = elbRecords.map(elb => future {
            client.elb.describeInstanceHealth(new DescribeInstanceHealthRequest(elb.id)).getInstanceStates.toList.map(
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
    val init = elbCrawler.addObserver(this)

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

class LaunchConfigurationCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeLaunchConfigurationsRequest
    override def doCrawl = {
        val it = new AwsIterator() {
            def next = {
                val response = client.asg.describeLaunchConfigurations.withNextToken(this.nextToken)
                this.nextToken = response.getNextToken
                response.getLaunchConfigurations.toList.map( 
                    item => Record.fromBean(item.getLaunchConfigurationName, item, new DateTime(item.getCreatedTime))
                )
            }
        }
        it.flatten.toList
    }
}

class ReservationCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeInstancesRequest
    override def doCrawl = client.ec2.describeInstances(request).getReservations.toList.map(
        item => Record.fromBean(item.getReservationId, item)
    )
}

case class InstanceCrawlerState(reservationRecords: List[Record] = List[Record]())

object InstanceCrawler extends StateMachine.LocalState[InstanceCrawlerState]

class InstanceCrawler(resCrawler: ReservationCrawler) extends Crawler {
    import InstanceCrawler._
    override def crawl() = resCrawler.crawl
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
    val init = resCrawler.addObserver(this)

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


class SecurityGroupCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeSecurityGroupsRequest
    override def doCrawl = client.ec2.describeSecurityGroups(request).getSecurityGroups.toList.map(
        item => Record.fromBean(item.getGroupId, item))
}

class SnapshotCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeSnapshotsRequest
    override def doCrawl = client.ec2.describeSnapshots(request).getSnapshots.toList.map(
        item => Record.fromBean(item.getSnapshotId, item, new DateTime(item.getStartTime))
    )
}

class TagCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeTagsRequest
    // TODO fromBean needs id which is a combo for tags
    override def doCrawl = client.ec2.describeTags(request).getTags.toList.map(
        item => Record.fromBean(item.getKey() + "|" + item.getResourceType() + "|" + item.getResourceId(), item)
    )
}

class VolumeCrawler(client: AwsClient) extends Crawler {
    val request = new DescribeVolumesRequest
    override def doCrawl = client.ec2.describeVolumes(request).getVolumes.toList.map(
        item => Record.fromBean(item.getVolumeId, item, new DateTime(item.getCreateTime))
    )
}

class BucketCrawler(client: AwsClient) extends Crawler {
    val request = new ListBucketsRequest
    override def doCrawl = client.s3.listBuckets(request).toList.map(
        item => Record.fromBean(item.getName, item, new DateTime(item.getCreationDate))
    )
}
