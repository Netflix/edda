package com.netflix.edda.aws

import com.netflix.edda.StateMachine
import com.netflix.edda.Crawler
import com.netflix.edda.CrawlerState
import com.netflix.edda.Observable
import com.netflix.edda.Record
import com.netflix.edda.BeanMapper
import com.netflix.edda.ConfigContext
import com.netflix.edda.Utils

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

trait AwsBeanMapper extends BeanMapper {
    val ctx: ConfigContext
    val instanceStateKeyMapper: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
        case (obj: com.amazonaws.services.ec2.model.InstanceState, "code", Some(value: Int)) => Some(0x00FF & value)
    }
    this.addKeyMapper(instanceStateKeyMapper)
    
    val tags = ctx.config.getProperty("edda.crawler.aws.suppressTags", "")
    tags.split(",").foreach( tag => {
        val pf: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
            case (obj: com.amazonaws.services.ec2.model.Tag, "value", Some(x: Any)) if obj.getKey() == tag => Some("[EDDA_SUPPRESSED]")
            case (obj: com.amazonaws.services.ec2.model.TagDescription, "value", Some(x: Any)) if obj.getKey() == tag => Some("[EDDA_SUPPRESSED]")
        }
        this.addKeyMapper(pf)
    })
}

// object AwsCrawlerBuilder {

//     def build(ctx : AwsCrawler.Context): Seq[AwsCrawler] = {
//         ctx.beanMapper.addKeyMapper(instanceStateKeyMapper)
//         val tags = ctx.config.getProperty("edda.crawler.aws.suppressTags", "")
//         tags.split(",").foreach( tag => {
//             val pf: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
//                 case (obj: com.amazonaws.services.ec2.model.Tag, "value", Some(x: Any)) if obj.getKey() == tag => Some("[EDDA_SUPPRESSED]")
//                 case (obj: com.amazonaws.services.ec2.model.TagDescription, "value", Some(x: Any)) if obj.getKey() == tag => Some("[EDDA_SUPPRESSED]")
//             }
//             ctx.beanMapper.addKeyMapper(pf)
//         })
        
        
//         val elb = new LoadBalancerCrawler(ctx)
//         val inst = new ReservationCrawler(ctx)

//         val aws = List(
//             new AddressCrawler(ctx),
//             new AutoScalingGroupCrawler(ctx),
//             new ImageCrawler(ctx),
//             new LaunchConfigurationCrawler(ctx),
//             new SecurityGroupCrawler(ctx),
//             new SnapshotCrawler(ctx),
//             new TagCrawler(ctx),
//             new VolumeCrawler(ctx),
//             new BucketCrawler(ctx),
//             elb,
//             inst
//         )
    
//         val views = List(
//             new InstanceHealthCrawler(ctx, elb),
//             new InstanceCrawler(ctx, inst)
//         )
            
//         views ++ aws
//     }
// }

abstract class AwsIterator  extends Iterator[Seq[Record]] {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    var nextToken: Option[String] = Some(null)
    def hasNext = nextToken != None
    def next: List[Record]
}

class AwsAddressCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeAddressesRequest
    override def doCrawl =
        ctx.awsClient.ec2.describeAddresses(request).getAddresses.asScala.map(
            item => Record(item.getPublicIp, ctx.beanMapper(item))
        ).toSeq
}

class AwsAutoScalingGroupCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    val request = new DescribeAutoScalingGroupsRequest
    request.setMaxRecords(50)
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
                ).toList
            }
        }
        val list = it.toList.flatten
        if(tagCount == 0) {
            if(Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
                throw new java.lang.RuntimeException("no tags found for any record in " + name + ", ignoring crawl results")
            }
            else logger.warn("no tags found for any record in " + name + ".  " +
                             "If you expect at least one tag then set: edda.crawler." + name + ".abortWithoutTags=true")
        }
        list
    }
}

class AwsImageCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeImagesRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeImages(request).getImages.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getImageId, ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class AwsLoadBalancerCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeLoadBalancersRequest
    override def doCrawl = ctx.awsClient.elb.describeLoadBalancers(request).getLoadBalancerDescriptions.asScala.map(
        item => Record(item.getLoadBalancerName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))
    ).toSeq
}

case class AwsInstanceHealthCrawlerState(elbRecords: Seq[Record] = Seq[Record]())

object AwsInstanceHealthCrawler extends StateMachine.LocalState[AwsInstanceHealthCrawlerState]

class AwsInstanceHealthCrawler(val name: String, val ctx : AwsCrawler.Context, val crawler: Crawler) extends Crawler(ctx) {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    import AwsInstanceHealthCrawler._
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
    def initState = addInitialState(super.initState, newLocalState(AwsInstanceHealthCrawlerState()))
    
    protected override
    def init = crawler.addObserver(this)

    protected
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawler.CrawlResult(from, elbRecords),state) => {
            // this is blocking so we dont crawl in parallel
            if( elbRecords ne localState(state).elbRecords ) {
                val newRecords = doCrawl(elbRecords)
                Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(this, newRecords) )
                setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceHealthCrawlerState(elbRecords))
            } else state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}

class AwsLaunchConfigurationCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeLaunchConfigurationsRequest
    request.setMaxRecords(50)
    override def doCrawl = {
        val it = new AwsIterator() {
            def next = {
                val response = ctx.awsClient.asg.describeLaunchConfigurations(request.withNextToken(this.nextToken.get))
                this.nextToken = Option(response.getNextToken)
                response.getLaunchConfigurations.asScala.map( 
                    item => Record(item.getLaunchConfigurationName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))
                ).toList
            }
        }
        it.toList.flatten
    }
}

class AwsReservationCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeInstancesRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeInstances(request).getReservations.asScala.map(
            item => {
                tagCount += item.getInstances.asScala.map( _.getTags.size ).sum
                Record(item.getReservationId, ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

case class AwsInstanceCrawlerState(reservationRecords: Seq[Record] = Seq[Record]())

object AwsInstanceCrawler extends StateMachine.LocalState[AwsInstanceCrawlerState]

class AwsInstanceCrawler(val name: String, val ctx : AwsCrawler.Context, val crawler: Crawler) extends Crawler(ctx) {
    import AwsInstanceCrawler._
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
    def initState = addInitialState(super.initState, newLocalState(AwsInstanceCrawlerState()))
    
    protected override
    def init = crawler.addObserver(this)

    protected
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawler.CrawlResult(from, reservations),state) => {
            // this is blocking so we dont crawl in parallel
            // TODO return state if elbRecords == state.elbRecords && minCycle not reached
            if( reservations ne localState(state).reservationRecords ) {
                val newRecords = doCrawl(reservations)
                Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(this, newRecords) )
                setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceCrawlerState(reservations))
            } else state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}


class AwsSecurityGroupCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeSecurityGroupsRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeSecurityGroups(request).getSecurityGroups.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getGroupId, ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class AwsSnapshotCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeSnapshotsRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeSnapshots(request).getSnapshots.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getSnapshotId, new DateTime(item.getStartTime), ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class AwsTagCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeTagsRequest
    override def doCrawl = ctx.awsClient.ec2.describeTags(request).getTags.asScala.map(
        item => Record(item.getKey() + "|" + item.getResourceType() + "|" + item.getResourceId(), ctx.beanMapper(item))
    ).toSeq
}

class AwsVolumeCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new DescribeVolumesRequest
    override def doCrawl = {
        var tagCount = 0
        val list = ctx.awsClient.ec2.describeVolumes(request).getVolumes.asScala.map(
            item => {
                tagCount += item.getTags.size
                Record(item.getVolumeId, new DateTime(item.getCreateTime), ctx.beanMapper(item))
            }
        ).toSeq
        if(tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
            throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
        }
        list
    }
}

class AwsBucketCrawler(val name: String, val ctx : AwsCrawler.Context) extends Crawler(ctx) {
    val request = new ListBucketsRequest
    override def doCrawl = ctx.awsClient.s3.listBuckets(request).asScala.map(
        item => Record(item.getName, new DateTime(item.getCreationDate), ctx.beanMapper(item))
    ).toSeq
}

// case class GroupAutoScalingGroupsCrawlerState(asgRecords: Seq[Record] = Seq(), slotMap: Map[String,Int] = Map())

// object GroupAutoScalingGroupsCrawler extends StateMachine.LocalState[GroupAutoScalingGroupsCrawlerState]

// class GroupAutoScalingGroupsCrawler(val name: String, val ctx : AwsCrawler.Context, val crawler: Crawler) extends Crawler(ctx) {
//     import GroupAutoScalingGroupsCrawler._
//     override def crawl() = Unit // we dont crawl, just get updates from crawler when it crawls
//     override def doCrawl = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on GroupAutoScalingGroupsCrawler")
//     def doCrawl(asgRecords: Seq[Record]): Seq[Record] = {
//         asgRecords.flatMap(rec => {
//             rec.data.asInstanceOf[Map[String,Any]].get("instances") match {
//                 case instances: Option[Seq[Map[String,Any]]] => instances.get.map(
//                     (inst: Map[String,Any]) => rec.copy(
//                         id=inst("instanceId").asInstanceOf[String],
//                         data=inst,
//                         ctime=inst("launchTime").asInstanceOf[DateTime]
//                     )
//                 )
//                 case other => throw new java.lang.RuntimeException("failed to crawl instances from reservation, got: " + other)
//             }
//         })
//     }

//     protected override
//     def initState = addInitialState(super.initState, newLocalState(GroupAutoScalingGroupsCrawlerState()))
    
//     protected override
//     def init = crawler.addObserver(this)

//     protected
//     def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
//         case (Crawler.CrawlResult(from, asgRecords),state) => {
//             // this is blocking so we dont crawl in parallel
//             // TODO return state if elbRecords == state.elbRecords && minCycle not reached
//             if( asgRecords ne localState(state).asgRecords ) {
//                 val newRecords = doCrawl(asgRecords)
//                 Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(this, newRecords) )
//                 setLocalState(
//                     Crawler.setLocalState(state, CrawlerState(newRecords)),
//                     GroupAutoScalingGroupsCrawlerState(asgRecords, asgRecords.map(rec => rec.id -> rec))
//                 )
//             } else state
//         }
//     }

//     override protected
//     def transitions = localTransitions orElse super.transitions
// }
