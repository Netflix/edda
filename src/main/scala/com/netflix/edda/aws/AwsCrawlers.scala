package com.netflix.edda.aws

import com.netflix.edda.StateMachine
import com.netflix.edda.Crawler
import com.netflix.edda.CrawlerState
import com.netflix.edda.Observable
import com.netflix.edda.Record
import com.netflix.edda.BeanMapper
import com.netflix.edda.ConfigContext

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


import collection.JavaConverters._
import scala.actors.Futures.{future, awaitAll}
import scala.actors.Actor

import org.slf4j.{Logger, LoggerFactory}

object AwsCrawler {
    trait Context extends ConfigContext {
        def awsClient: AwsClient
        def beanMapper: BeanMapper
    }
}

object AwsCrawlerBuilder {

    val instanceStateKeyMapper: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
        case (obj: com.amazonaws.services.ec2.model.InstanceState, "code", Some(value: Int)) => Some(0x00FF & value)
    }

    
    def build(ctx : AwsCrawler.Context): Seq[AwsCrawler] = {
        ctx.beanMapper.addKeyMapper(instanceStateKeyMapper)
        val tags = ctx.config.getProperty("edda.crawler.aws.suppressTags", "")
        tags.split(",").foreach( tag => {
            val pf: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
                case (obj: com.amazonaws.services.ec2.model.Tag, "value", Some(x: Any)) if obj.getKey() == tag => Some("[EDDA_SUPPRESSED]")
                case (obj: com.amazonaws.services.ec2.model.TagDescription, "value", Some(x: Any)) if obj.getKey() == tag => Some("[EDDA_SUPPRESSED]")
            }
            ctx.beanMapper.addKeyMapper(pf)
        })
        
        
        val elb = new LoadBalancerCrawler(ctx)
        val inst = new ReservationCrawler(ctx)

        val aws = List(
            new AddressCrawler(ctx),
            new AutoScalingGroupCrawler(ctx),
            new ImageCrawler(ctx),
            new LaunchConfigurationCrawler(ctx),
            new SecurityGroupCrawler(ctx),
            new SnapshotCrawler(ctx),
            new TagCrawler(ctx),
            new VolumeCrawler(ctx),
            new BucketCrawler(ctx),
            elb,
            inst
        )
    
        val views = List(
            new InstanceHealthCrawler(ctx, elb),
            new InstanceCrawler(ctx, inst)
        )
            
        views ++ aws
    }
}

abstract class AwsIterator  extends Iterator[Seq[Record]] {
    var nextToken: Option[String] = Some(null)
    def hasNext = nextToken != None
    def next: Seq[Record]
}

abstract class AwsCrawler(val rootName: String, ctx: AwsCrawler.Context) extends Crawler(ctx) {
    val name = ctx.awsClient.accountName match {
        case "" => rootName
        case x: String => x + "." + rootName
    }
}

class AddressCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.addresses", ctx) {
    val request = new DescribeAddressesRequest
    override def doCrawl =
        ctx.awsClient.ec2.describeAddresses(request).getAddresses.asScala.map(
            item => Record(item.getPublicIp, ctx.beanMapper(item))
        ).toSeq
}

class AutoScalingGroupCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.autoScalingGroups", ctx) {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    val request = new DescribeAutoScalingGroupsRequest
    override def doCrawl = {
        var tagCount = 0
        val it = new AwsIterator() {
            def next = {
                val response = ctx.awsClient.asg.describeAutoScalingGroups(request.withNextToken(this.nextToken.get))
                this.nextToken = Option(response.getNextToken)
                response.getAutoScalingGroups.asScala.map(
                    item => {
                        tagCount += item.getTags.size
                        Record(item.getAutoScalingGroupName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))
                    }
                ).toSeq
            }
        }
        val list = it.flatten.toSeq
        if(tagCount == 0) {
            if(ctx.config.getProperty("edda.crawler." + name + ".abortWithoutTags", "false").toBoolean) {
                throw new java.lang.RuntimeException("no tags found for any record in " + name + ", ignoring crawl results")
            }
            else logger.warn("no tags found for any record in " + name + ".  " +
                             "If you expect at least one tag then set: edda.crawler." + name + ".abortWithoutTags=true")
        }
        list
    }
}

class ImageCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.images", ctx) {
    val request = new DescribeImagesRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeImages(request).getImages.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getImageId, ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && ctx.config.getProperty("edda.crawler." + name + ".abortWithoutTags", "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class LoadBalancerCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.loadBalancers", ctx) {
    val request = new DescribeLoadBalancersRequest
    override def doCrawl = ctx.awsClient.elb.describeLoadBalancers(request).getLoadBalancerDescriptions.asScala.map(
        item => Record(item.getLoadBalancerName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))
    ).toSeq
}

case class InstanceHealthCrawlerState(elbRecords: Seq[Record] = Seq[Record]())

object InstanceHealthCrawler extends StateMachine.LocalState[InstanceHealthCrawlerState]

class InstanceHealthCrawler(val ctx : AwsCrawler.Context, val crawler: Crawler) extends AwsCrawler("view.loadBalancerInstances", ctx) {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    import InstanceHealthCrawler._
    override def crawl() = Unit // we dont crawl, just get updates from crawler when it crawls
    override def doCrawl = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceHealthCrawler")
    def doCrawl(elbRecords: Seq[Record]): Seq[Record] = {
        val tasks = elbRecords.map(elb => future {
            val instances = ctx.awsClient.elb.describeInstanceHealth(new DescribeInstanceHealthRequest(elb.id)).getInstanceStates
            elb.copy(data=Map("name" -> elb.id, "instances" -> instances.asScala.map(ctx.beanMapper(_))))
        })
        awaitAll(300000L, tasks:_*) match { 
            case Nil => Seq()
            case x: Seq[Option[Record]] => x.collect {  case Some(d) => d }
        }
    }

    protected override
    def initState = addInitialState(super.initState, newLocalState(InstanceHealthCrawlerState()))
    
    protected override
    def init = crawler.addObserver(this)

    protected
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawler.CrawlResult(elbRecords),state) => {
            // this is blocking so we dont crawl in parallel
            if( elbRecords ne localState(state).elbRecords ) {
                val newRecords = doCrawl(elbRecords)
                Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
                setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), InstanceHealthCrawlerState(elbRecords))
            } else state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}

class LaunchConfigurationCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.launchConfigurations", ctx) {
    val request = new DescribeLaunchConfigurationsRequest
    override def doCrawl = {
        val it = new AwsIterator() {
            def next = {
                val response = ctx.awsClient.asg.describeLaunchConfigurations.withNextToken(this.nextToken.get)
                this.nextToken = Option(response.getNextToken)
                response.getLaunchConfigurations.asScala.map( 
                    item => Record(item.getLaunchConfigurationName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))
                ).toSeq
            }
        }
        it.flatten.toSeq
    }
}

class ReservationCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.instances", ctx) {
    val request = new DescribeInstancesRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeInstances(request).getReservations.asScala.map(
            item => {
                tagCount += item.getInstances.asScala.map( _.getTags.size ).sum
                Record(item.getReservationId, ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && ctx.config.getProperty("edda.crawler." + name + ".abortWithoutTags", "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

case class InstanceCrawlerState(reservationRecords: Seq[Record] = Seq[Record]())

object InstanceCrawler extends StateMachine.LocalState[InstanceCrawlerState]

class InstanceCrawler(val ctx : AwsCrawler.Context, val crawler: Crawler) extends AwsCrawler("view.instances", ctx) {
    import InstanceCrawler._
    override def crawl() = Unit // we dont crawl, just get updates from crawler when it crawls
    override def doCrawl = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceCrawler")
    def doCrawl(resRecords: Seq[Record]): Seq[Record] = {
        resRecords.flatMap(rec => {
            rec.data.asInstanceOf[Map[String,Any]].get("instances") match {
                case instances: Option[Seq[Map[String,Any]]] => instances.get.map(
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
    def init = crawler.addObserver(this)

    protected
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawler.CrawlResult(reservations),state) => {
            // this is blocking so we dont crawl in parallel
            // TODO return state if elbRecords == state.elbRecords && minCycle not reached
            if( reservations ne localState(state).reservationRecords ) {
                val newRecords = doCrawl(reservations)
                Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
                setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), InstanceCrawlerState(reservations))
            } else state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}


class SecurityGroupCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.securityGroups", ctx) {
    val request = new DescribeSecurityGroupsRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeSecurityGroups(request).getSecurityGroups.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getGroupId, ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && ctx.config.getProperty("edda.crawler." + name + ".abortWithoutTags", "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class SnapshotCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.snapshots", ctx) {
    val request = new DescribeSnapshotsRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeSnapshots(request).getSnapshots.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getSnapshotId, new DateTime(item.getStartTime), ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && ctx.config.getProperty("edda.crawler." + name + ".abortWithoutTags", "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class TagCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.tags", ctx) {
    val request = new DescribeTagsRequest
    override def doCrawl = ctx.awsClient.ec2.describeTags(request).getTags.asScala.map(
        item => Record(item.getKey() + "|" + item.getResourceType() + "|" + item.getResourceId(), ctx.beanMapper(item))
    ).toSeq
}

class VolumeCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.volumes", ctx) {
    val request = new DescribeVolumesRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeVolumes(request).getVolumes.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getVolumeId, new DateTime(item.getCreateTime), ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && ctx.config.getProperty("edda.crawler." + name + ".abortWithoutTags", "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class BucketCrawler(val ctx : AwsCrawler.Context) extends AwsCrawler("aws.buckets", ctx) {
    val request = new ListBucketsRequest
    override def doCrawl = ctx.awsClient.s3.listBuckets(request).asScala.map(
        item => Record(item.getName, new DateTime(item.getCreationDate), ctx.beanMapper(item))
    ).toSeq
}
