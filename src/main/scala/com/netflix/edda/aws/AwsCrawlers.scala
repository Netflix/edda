/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda.aws

import scala.actors.Actor

import com.netflix.edda.StateMachine
import com.netflix.edda.Crawler
import com.netflix.edda.CrawlerState
import com.netflix.edda.Observable
import com.netflix.edda.Record
import com.netflix.edda.RecordSet
import com.netflix.edda.RequestId
import com.netflix.edda.BeanMapper
import com.netflix.edda.basic.BasicBeanMapper
import com.netflix.edda.Utils
import com.netflix.edda.ObserverExecutionContext

import org.joda.time.DateTime

import com.amazonaws.AmazonServiceException

import com.amazonaws.services.ec2.model.DescribeAddressesRequest
import com.amazonaws.services.ec2.model.DescribeImagesRequest
import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.DescribeReservedInstancesRequest
import com.amazonaws.services.ec2.model.DescribeReservedInstancesOfferingsRequest
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest
import com.amazonaws.services.ec2.model.DescribeTagsRequest
import com.amazonaws.services.ec2.model.DescribeVolumesRequest
import com.amazonaws.services.ec2.model.DescribeVpcsRequest

import com.amazonaws.services.identitymanagement.model.ListUsersRequest
import com.amazonaws.services.identitymanagement.model.ListAccessKeysRequest
import com.amazonaws.services.identitymanagement.model.ListGroupsForUserRequest
import com.amazonaws.services.identitymanagement.model.ListUserPoliciesRequest
import com.amazonaws.services.identitymanagement.model.ListGroupsRequest
import com.amazonaws.services.identitymanagement.model.ListGroupPoliciesRequest
import com.amazonaws.services.identitymanagement.model.ListRolesRequest
import com.amazonaws.services.identitymanagement.model.ListVirtualMFADevicesRequest

import com.amazonaws.services.s3.model.ListBucketsRequest

import com.amazonaws.services.sqs.model.ListQueuesRequest
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest

import com.amazonaws.services.cloudwatch.model.DescribeAlarmsRequest

import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsRequest
import com.amazonaws.services.autoscaling.model.DescribePoliciesRequest
import com.amazonaws.services.autoscaling.model.DescribeScalingActivitiesRequest
import com.amazonaws.services.autoscaling.model.DescribeScheduledActionsRequest

import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest
import com.amazonaws.services.elasticloadbalancing.model.DescribeInstanceHealthRequest

import com.amazonaws.services.route53.model.ListHostedZonesRequest
import com.amazonaws.services.route53.model.GetHostedZoneRequest
import com.amazonaws.services.route53.model.ListResourceRecordSetsRequest

import collection.JavaConverters._

import java.util.concurrent.Executors
import java.util.concurrent.Callable

import org.slf4j.LoggerFactory
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest
import com.amazonaws.services.elasticache.model.DescribeCacheClustersRequest
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest
import com.amazonaws.services.cloudformation.model.ListStackResourcesRequest

/** static namespace for out Context trait */
object AwsCrawler {

  /** All AWS Crawlers require the basic ConfigContext
    * plus an [[com.netflix.edda.aws.AwsClient]] and [[com.netflix.edda.BeanMapper]]
    */
  trait Context {
    def awsClient: AwsClient

    def beanMapper: BeanMapper
  }

}

/** specialized [[com.netflix.edda.BeanMapper]] trait that can suppress specific AWS resource tags
  * based on patterns expressed in the config.  This is necessary in case people add tags to
  * resources that change frequenly (like timestamps).  The AwsBeanMapper trait also works around
  * some internal state exposed in the bean for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/model/InstanceState.html com.amazonaws.services.ec2.model.InstanceState]]
  * *code* field.
  */
trait AwsBeanMapper extends BeanMapper {
  val basicBeanMapper = new BasicBeanMapper
  val suppressSet = Utils.getProperty("edda.crawler", "aws.suppressTags", "", "").get.split(",").toSet

  val suppressKeyMapper: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
    case (obj: com.amazonaws.services.ec2.model.Tag, "value", Some(x: Any)) if suppressSet.contains(obj.getKey) => Some("[EDDA_SUPPRESSED]")
    case (obj: com.amazonaws.services.ec2.model.TagDescription, "value", Some(x: Any)) if suppressSet.contains(obj.getKey) => Some("[EDDA_SUPPRESSED]")
    case (obj: com.amazonaws.services.autoscaling.model.Tag, "value", Some(x: Any)) if suppressSet.contains(obj.getKey) => Some("[EDDA_SUPPRESSED]")
    case (obj: com.amazonaws.services.autoscaling.model.TagDescription, "value", Some(x: Any)) if suppressSet.contains(obj.getKey) => Some("[EDDA_SUPPRESSED]")
  }
  basicBeanMapper.addKeyMapper(suppressKeyMapper)

  def flattenTag(obj: Map[String,Any]) = obj + (obj("key").asInstanceOf[String] -> obj("value"))

  // this will flatten the tags so that we will have: { key -> a, value -> b, a -> b }
  val tagObjMapper: PartialFunction[AnyRef,AnyRef] = {
    case obj : com.amazonaws.services.ec2.model.Tag =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : com.amazonaws.services.ec2.model.TagDescription =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : com.amazonaws.services.autoscaling.model.Tag =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : com.amazonaws.services.autoscaling.model.TagDescription =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
  }
  this.addObjMapper(tagObjMapper)

  val instanceStateKeyMapper: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
    case (obj: com.amazonaws.services.ec2.model.InstanceState, "code", Some(value: Int)) => Some(0x00FF & value)
  }
  this.addKeyMapper(instanceStateKeyMapper)

}

/** iterator interface for working with the paginated results from some
  * aws apis
  */
abstract class AwsIterator extends Iterator[Seq[Record]] {
  var nextToken: Option[String] = Some(null)

  def hasNext = nextToken != None

  def next(): List[Record]
}

/** crawler for EIP Addresses
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsAddressCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeAddressesRequest

  override def doCrawl()(implicit req: RequestId) =
    ctx.awsClient.ec2.describeAddresses(request).getAddresses.asScala.map(
      item => Record(item.getPublicIp, ctx.beanMapper(item))).toSeq
}

/** crawler for AutoScalingGroups
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsAutoScalingGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeAutoScalingGroupsRequest
  request.setMaxRecords(50)

  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")
  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val it = new AwsIterator() {
      def next() = {
        val response = ctx.awsClient.asg.describeAutoScalingGroups(request.withNextToken(this.nextToken.get))
        this.nextToken = Option(response.getNextToken)
        response.getAutoScalingGroups.asScala.map(
          item => {
            tagCount += item.getTags.size
            Record(item.getAutoScalingGroupName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))
          }).toList
      }
    }
    val list = it.toList.flatten
    if (tagCount == 0) {
      if (abortWithoutTags.get.toBoolean) {
        throw new java.lang.RuntimeException("no tags found for any record in " + name + ", ignoring crawl results")
      } else if (logger.isWarnEnabled) logger.warn(s"$req no tags found for any record in $name.  If you expect at least one tag then set: edda.crawler.$name.abortWithoutTags=true")
    }
    list
  }
}

/** crawler for ASG Policies
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsScalingPolicyCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribePoliciesRequest
  request.setMaxRecords(50)

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = ctx.awsClient.asg.describePolicies(request.withNextToken(this.nextToken.get))
        this.nextToken = Option(response.getNextToken)
        response.getScalingPolicies.asScala.map(
          item => {
            Record(item.getPolicyName, ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for ASG Activities
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsScalingActivitiesCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeScalingActivitiesRequest
  request.setMaxRecords(50)

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = ctx.awsClient.asg.describeScalingActivities(request.withNextToken(this.nextToken.get))
        this.nextToken = Option(response.getNextToken)
        response.getActivities.asScala.map(
          item => {
            Record(item.getActivityId, ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for ASG Scheduled Actions
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsScheduledActionsCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeScheduledActionsRequest
  request.setMaxRecords(50)

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = ctx.awsClient.asg.describeScheduledActions(request.withNextToken(this.nextToken.get))
        this.nextToken = Option(response.getNextToken)
        response.getScheduledUpdateGroupActions.asScala.map(
          item => {
            Record(item.getScheduledActionARN, ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for ASG VPCs
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsVpcCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeVpcsRequest

  override def doCrawl()(implicit req: RequestId) = {
    val response = ctx.awsClient.ec2.describeVpcs()
    response.getVpcs.asScala.map(
      item => {
        Record(item.getVpcId, ctx.beanMapper(item))
      }).toList
  }
}


/** crawler for CLoudWatch Alarms
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsAlarmCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeAlarmsRequest
  request.setMaxRecords(100)

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = ctx.awsClient.cw.describeAlarms(request.withNextToken(this.nextToken.get))
        this.nextToken = Option(response.getNextToken)
        response.getMetricAlarms.asScala.map(
          item => {
            Record(item.getAlarmName, ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for Images
  *
  * if tags are used with on the aws images objects, set edda.crawler.NAME.abortWithoutTags=true
  * so that the crawler can detect when AWS sends a degraded result without tag information.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsImageCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeImagesRequest
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeImages(request).getImages.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getImageId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for LoadBalancers
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsLoadBalancerCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeLoadBalancersRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.elb.describeLoadBalancers(request).getLoadBalancerDescriptions.asScala.map(
    item => Record(item.getLoadBalancerName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))).toSeq
}

case class AwsInstanceHealthCrawlerState(elbRecords: Seq[Record] = Seq[Record]())

object AwsInstanceHealthCrawler extends StateMachine.LocalState[AwsInstanceHealthCrawlerState]

/** crawler for LoadBalancer Instances
  *
  * this is a secondary crawler that takes the results from the AwsLoadBalancerCrawler
  * and then crawls the instance states for each ELB.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  * @param crawler the LoadBalancer crawler
  */
class AwsInstanceHealthCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler {

  import AwsInstanceHealthCrawler._

  override def crawl()(implicit req: RequestId) {}

  // we don't crawl, just get updates from crawler when it crawls
  override def doCrawl()(implicit req: RequestId) = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceHealthCrawler")

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  /** for each elb call describeInstanceHealth and map that to a new document
    *
    * @param elbRecords the records to crawl
    * @return the record set for the instanceHealth
    */
  def doCrawl(elbRecords: Seq[Record])(implicit req: RequestId): Seq[Record] = {
    val futures: Seq[java.util.concurrent.Future[Record]] = elbRecords.map(
      elb => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              try {
                val instances = ctx.awsClient.elb.describeInstanceHealth(new DescribeInstanceHealthRequest(elb.id)).getInstanceStates
                elb.copy(data = Map("name" -> elb.id, "instances" -> instances.asScala.map(ctx.beanMapper(_))))
              } catch {
                case e: Exception => {
                  throw new java.lang.RuntimeException(this + " describeInstanceHealth failed for ELB " + elb.id, e)
                }
              }
            }
          }
        )
      }
    )
    var failed: Boolean = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            failed = true
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from describeInstanceHealth", e)
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl instance health")
    }
    records
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsInstanceHealthCrawlerState()))

  protected override def init() {
    implicit val req = RequestId("init")
    import Utils._
    Utils.namedActor(this + " init") {
      import ObserverExecutionContext._
      crawler.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req${Actor.self} failed to add observer $this to $crawler: $msg, retrying")
          this.init
        }
        case scala.util.Success(msg) => super.init
      }
    }
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawler.CrawlResult(from, elbRecordSet), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we dont crawl in parallel
      if (elbRecordSet.records ne localState(state).elbRecords) {
        val newRecords = doCrawl(elbRecordSet.records)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id))))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceHealthCrawlerState(elbRecordSet.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}


/** crawler for LaunchConfigurations
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsLaunchConfigurationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeLaunchConfigurationsRequest
  request.setMaxRecords(50)

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = ctx.awsClient.asg.describeLaunchConfigurations(request.withNextToken(this.nextToken.get))
        this.nextToken = Option(response.getNextToken)
        response.getLaunchConfigurations.asScala.map(
          item => Record(item.getLaunchConfigurationName, new DateTime(item.getCreatedTime), ctx.beanMapper(item))).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for Reservations (ie group of instances, not pre-paid reserved instances)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsReservationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeInstancesRequest

  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")
  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeInstances(request).getReservations.asScala.map(
      item => {
        tagCount += item.getInstances.asScala.map(_.getTags.size).sum
        Record(item.getReservationId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

case class AwsInstanceCrawlerState(reservationRecords: Seq[Record] = Seq[Record]())

object AwsInstanceCrawler extends StateMachine.LocalState[AwsInstanceCrawlerState]

/** crawler for Instances
  *
  * this is a secondary crawler that takes the results from the AwsReservationCrawler
  * and then pulls out each instance in the reservations to track them seperately
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  * @param crawler the AwsReservation crawler
  */
class AwsInstanceCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler {

  import AwsInstanceCrawler._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  override def crawl()(implicit req: RequestId) {}

  // we dont crawl, just get updates from crawler when it crawls
  override def doCrawl()(implicit req: RequestId) = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceCrawler")

  def doCrawl(resRecords: Seq[Record]): Seq[Record] = {
    resRecords.flatMap(rec => {
      rec.data.asInstanceOf[Map[String, Any]].get("instances") match {
        case Some(instances: Seq[_]) => instances.asInstanceOf[Seq[Map[String,Any]]].map(
          (inst: Map[String, Any]) => rec.copy(
            id = inst("instanceId").asInstanceOf[String],
            data = inst,
            ctime = inst("launchTime").asInstanceOf[DateTime]))
        case other => throw new java.lang.RuntimeException("failed to crawl instances from reservation, got: " + other)
      }
    })
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsInstanceCrawlerState()))

  protected override def init() {
    implicit val req = RequestId("init")
    import Utils._
    Utils.namedActor(this + " init") {
      import ObserverExecutionContext._
      crawler.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req{Actor.self} failed to add observer $this $crawler: $msg, retrying")
          this.init
        }
        case scala.util.Success(msg) => super.init
      }
    }
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawler.CrawlResult(from, reservationSet), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we dont crawl in parallel
      if (reservationSet.records ne localState(state).reservationRecords) {
        val newRecords = doCrawl(reservationSet.records)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id))))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceCrawlerState(reservationSet.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}

/** crawler for Security Group
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSecurityGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeSecurityGroupsRequest
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeSecurityGroups(request).getSecurityGroups.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getGroupId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for Snapshots
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSnapshotCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeSnapshotsRequest
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeSnapshots(request).getSnapshots.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getSnapshotId, new DateTime(item.getStartTime), ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for all Tags
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsTagCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeTagsRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.ec2.describeTags(request).getTags.asScala.map(
    item => Record(item.getKey + "|" + item.getResourceType + "|" + item.getResourceId, ctx.beanMapper(item))).toSeq
}

/** crawler for Volumes
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsVolumeCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeVolumesRequest
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeVolumes(request).getVolumes.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getVolumeId, new DateTime(item.getCreateTime), ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for S3 Buckets
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsBucketCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListBucketsRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.s3.listBuckets(request).asScala.map(
    item => Record(item.getName, new DateTime(item.getCreationDate), ctx.beanMapper(item))).toSeq
}

/** crawler for IAM Users
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamUserCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListUsersRequest
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl()(implicit req: RequestId) = {
    val users = ctx.awsClient.identitymanagement.listUsers(request).getUsers.asScala
    val futures: Seq[java.util.concurrent.Future[Record]] = users.map(
      user => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val groupsRequest = new ListGroupsForUserRequest().withUserName(user.getUserName)
              val groups = ctx.awsClient.identitymanagement.listGroupsForUser(groupsRequest).getGroups.asScala.map( item => item.getGroupName ).toSeq
              val accessKeysRequest = new ListAccessKeysRequest().withUserName(user.getUserName)
              val accessKeys = Map[String, String]() ++ ctx.awsClient.identitymanagement.listAccessKeys(accessKeysRequest).getAccessKeyMetadata.asScala.map(item => ctx.beanMapper(item)).toSeq
              val userPoliciesRequest = new ListUserPoliciesRequest().withUserName(user.getUserName)
              val userPolicies = ctx.awsClient.identitymanagement.listUserPolicies(userPoliciesRequest).getPolicyNames.asScala
              Record(user.getUserName, new DateTime(user.getCreateDate), Map("name" -> user.getUserName, "attributes" -> (ctx.beanMapper(user)), "groups" -> groups, "accessKeys" -> accessKeys, "userPolicies" -> userPolicies))
            }
          }
        )
      }
    )
    var failed = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from IAM user sub requests", e)
            failed = true
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }

}

/** crawler for IAM Groups
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListGroupsRequest
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl()(implicit req: RequestId) = {
    val groups = ctx.awsClient.identitymanagement.listGroups(request).getGroups.asScala
    val futures: Seq[java.util.concurrent.Future[Record]] = groups.map(
      group => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val groupPoliciesRequest = new ListGroupPoliciesRequest().withGroupName(group.getGroupName)
              val groupPolicies = ctx.awsClient.identitymanagement.listGroupPolicies(groupPoliciesRequest).getPolicyNames.asScala.toSeq
              Record(group.getGroupName, new DateTime(group.getCreateDate), Map("name" -> group.getGroupName, "attributes" -> (ctx.beanMapper(group)), "policies" -> groupPolicies))
            }
          }
        )
      }
    )
    var failed = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from IAM listGroupPolicies", e)
            failed = true
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }

}

/** crawler for IAM Roles
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamRoleCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListRolesRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.identitymanagement.listRoles(request).getRoles.asScala.map(
    item => Record(item.getRoleName, new DateTime(item.getCreateDate), ctx.beanMapper(item))).toSeq
}

/** crawler for IAM Virtual MFA Devices
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamVirtualMFADeviceCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListVirtualMFADevicesRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.identitymanagement.listVirtualMFADevices(request).getVirtualMFADevices.asScala.map(
    item => Record(item.getSerialNumber.split('/').last, new DateTime(item.getEnableDate), ctx.beanMapper(item))).toSeq
}

/** crawler for SQS Queues
  *
  * This crawler is similar to the InstanceHealth crawler in that it has to first
  * get a list of SQS Queues then for each queue fan-out and query each queue.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSimpleQueueCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListQueuesRequest
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl()(implicit req: RequestId) = {
    val queues = ctx.awsClient.sqs.listQueues(request).getQueueUrls.asScala
    val futures: Seq[java.util.concurrent.Future[Record]] = queues.map(
      queueUrl => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val name = queueUrl.split('/').last
              val attrRequest = new GetQueueAttributesRequest().withQueueUrl(queueUrl).withAttributeNames("All")
              val attrs = Map[String, String]() ++ ctx.awsClient.sqs.getQueueAttributes(attrRequest).getAttributes.asScala
              val ctime = attrs.get("CreatedTimestamp") match {
                case Some(time) => new DateTime(time.toInt * 1000)
                case None => DateTime.now
              }

              Record(name, ctime, Map("name" -> name, "url" -> queueUrl, "attributes" -> (attrs)))
            }
          }
        )
      }
    )
    var failed: Boolean = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: java.util.concurrent.ExecutionException => {
            e.getCause match {
              case e: AmazonServiceException if e.getErrorCode == "AWS.SimpleQueueService.NonExistentQueue" => {
                // this happens constantly, dont log it.  There is a large time delta between queues being deleted
                // but still showing up in the ListQueuesResult
                None
              }
              case e: Throwable => {
                if (logger.isErrorEnabled) logger.error(s"$req$this exception from SQS getQueueAttributes", e)
                failed = true
                None
              }
            }
          }
          case e: Throwable => {
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from SQS getQueueAttributes", e)
            failed = true
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }
}

/** crawler for ReservedInstance (ie pre-paid instances)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsReservedInstanceCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeReservedInstancesRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.ec2.describeReservedInstances(request).getReservedInstances.asScala.map(
    item => Record(item.getReservedInstancesId, new DateTime(item.getStart), ctx.beanMapper(item))).toSeq
}

/** crawler for ReservedInstancesOfferings
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsReservedInstancesOfferingCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeReservedInstancesOfferingsRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.ec2.describeReservedInstancesOfferings(request).getReservedInstancesOfferings.asScala.map(
    item => Record(item.getReservedInstancesOfferingId, ctx.beanMapper(item))).toSeq
}

/** crawler for Route53 Hosted Zones (DNS records)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsHostedZoneCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new ListHostedZonesRequest

  override def doCrawl()(implicit req: RequestId) =  ctx.awsClient.route53.listHostedZones(request).getHostedZones.asScala.map(
      item => Record(item.getName, ctx.beanMapper(item))).toSeq
}

case class AwsHostedRecordCrawlerState(hostedZones: Seq[Record] = Seq[Record]())

object AwsHostedRecordCrawler extends StateMachine.LocalState[AwsHostedRecordCrawlerState]

/** crawler for Route53 Resource Record Sets (DNS records)
  *  this is a secondary crawler that crawls the resource recordsets for each hosted zone
  * and then pulls out each recordset in the zones to track them seperately
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  * @param crawler the awsHostedZone crawler
  */
class AwsHostedRecordCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler {

  import AwsHostedRecordCrawler._

  override def crawl()(implicit req: RequestId) {}

  // we dont crawl, just get updates from crawler when it crawls
  override def doCrawl()(implicit req: RequestId) = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on HostedRecordCrawler")

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)
  /** for each zone call listResourceRecordSets and map that to a new document
    *
    * @param zones the records to crawl
    * @return the record set for the resourceRecordSet
    */
  def doCrawl(zones: Seq[Record])(implicit req: RequestId): Seq[Record] = {

    val futures: Seq[java.util.concurrent.Future[Seq[Record]]] = zones.map(
      zone => {
        val zoneId = zone.data.asInstanceOf[Map[String,Any]]("id").asInstanceOf[String]
        val zoneName = zone.id
        val request = new ListResourceRecordSetsRequest(zoneId)
        threadPool.submit(
          new Callable[Seq[Record]] {
            def call() = {
              val it = new AwsIterator() {
                def next() = {
                  val response = ctx.awsClient.route53.listResourceRecordSets(request.withStartRecordName(this.nextToken.get))
                  this.nextToken = Option(response.getNextRecordName)
                  response.getResourceRecordSets.asScala.map(
                    item => {
                      Record(item.getName, ctx.beanMapper(item).asInstanceOf[Map[String,Any]] ++ Map("zone" -> Map("id" -> zoneId, "name" -> zoneName)))
                    }
                  ).toList
                }
              }
              it.toList.flatten
            }
          }
        )
      }
    )
    var failed: Boolean = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            failed = true
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from listResourceRecordSets", e)
            None
          }
        }
      }
    ).collect({
      case Some(rec) => rec
    }).flatten

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsHostedRecordCrawlerState()))

  protected override def init() {
    implicit val req = RequestId("init")
    import Utils._
    Utils.namedActor(this + " init") {
      import ObserverExecutionContext._
      crawler.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req${Actor.self} failed to add observer $this to $crawler: $msg, retrying")
          this.init
        }
        case scala.util.Success(msg) => super.init
      }
    }
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawler.CrawlResult(from, hostedZoneSet), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we dont crawl in parallel
      if (hostedZoneSet.records ne localState(state).hostedZones) {
        val newRecords = doCrawl(hostedZoneSet.records)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id))))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsHostedRecordCrawlerState(hostedZoneSet.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}

/** crawler for RDS Databases
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsDatabaseCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeDBInstancesRequest
  request.setMaxRecords(50)

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        // annoying, describeDBInstances has withMarker and getMarker instead if withToken and getNextToken
        val response = ctx.awsClient.rds.describeDBInstances(request.withMarker(this.nextToken.get))
        this.nextToken = Option(response.getMarker)
        response.getDBInstances.asScala.map(
          item => {
            Record(item.getDBInstanceIdentifier, ctx.beanMapper(item))
          }).toList
      }
    }
    val list = it.toList.flatten
    list
  }
}

/** crawler for ElastiCache Clusters
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsCacheClusterCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeCacheClustersRequest

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.elasticache.describeCacheClusters(request).getCacheClusters.asScala.map(
    item => Record(item.getCacheClusterId, ctx.beanMapper(item))).toSeq
}

/** crawler for Subnets
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSubnetCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeSubnetsRequest
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeSubnets(request).getSubnets.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getSubnetId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for Cloudformation Stacks
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsCloudformationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = new DescribeStacksRequest
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(1)

  override def doCrawl()(implicit req: RequestId) = {
    val stacks = ctx.awsClient.cloudformation.describeStacks(request).getStacks.asScala
    val futures: Seq[java.util.concurrent.Future[Record]] = stacks.map(
      stack => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val stackResourcesRequest = new ListStackResourcesRequest().withStackName(stack.getStackName)
              val stackResources = ctx.awsClient.cloudformation.listStackResources(stackResourcesRequest).getStackResourceSummaries.asScala.map(item => ctx.beanMapper(item)).toSeq
              Record(stack.getStackName, new DateTime(stack.getCreationTime), ctx.beanMapper(stack).asInstanceOf[Map[String,Any]] ++ Map("resources" -> stackResources))
            }
          }
        )
      }
    )
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(this + "exception from Cloudformation listStackResources", e)
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    records
  }

}
