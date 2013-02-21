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
import com.amazonaws.services.ec2.model.DescribeReservedInstancesRequest
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest
import com.amazonaws.services.ec2.model.DescribeTagsRequest
import com.amazonaws.services.ec2.model.DescribeVolumesRequest

import com.amazonaws.services.s3.model.ListBucketsRequest

import com.amazonaws.services.sqs.model.ListQueuesRequest
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest

import com.amazonaws.services.cloudwatch.model.DescribeAlarmsRequest

import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import com.amazonaws.services.autoscaling.model.DescribeLaunchConfigurationsRequest
import com.amazonaws.services.autoscaling.model.DescribePoliciesRequest

import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest
import com.amazonaws.services.elasticloadbalancing.model.DescribeInstanceHealthRequest

import com.amazonaws.services.route53.model.ListHostedZonesRequest
import com.amazonaws.services.route53.model.GetHostedZoneRequest
import com.amazonaws.services.route53.model.ListResourceRecordSetsRequest

import collection.JavaConverters._

import java.util.concurrent.Executors
import java.util.concurrent.Callable

import org.slf4j.LoggerFactory

/** static namespace for out Context trait */
object AwsCrawler {

  /** All AWS Crawlers require the basic ConfigContext
    * plus an [[com.netflix.edda.aws.AwsClient]] and [[com.netflix.edda.BeanMapper]]
    */
  trait Context extends ConfigContext {
    def awsClient: AwsClient

    def beanMapper: BeanMapper
  }

}

/** specialized [[com.netflix.edda.BeanMapper]] trait that can suppress specific AWS resource tags
  * based on patterns expressed in the config.  This is necessary in case people add tags to
  * resources that change frequenly (like timestamps).  The AwsBeanMapper trait also works around
  * some internal state exposed in the bean for [[com.amazonaws.services.ec2.model.InstanceState]]
  * *code* field.
  */
trait AwsBeanMapper extends BeanMapper {
  def ctx: ConfigContext

  val instanceStateKeyMapper: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
    case (obj: com.amazonaws.services.ec2.model.InstanceState, "code", Some(value: Int)) => Some(0x00FF & value)
  }
  this.addKeyMapper(instanceStateKeyMapper)

  val tags = ctx.config.getProperty("edda.crawler.aws.suppressTags", "")
  tags.split(",").foreach(tag => {
    val pf: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj: com.amazonaws.services.ec2.model.Tag, "value", Some(x: Any)) if obj.getKey == tag => Some("[EDDA_SUPPRESSED]")
      case (obj: com.amazonaws.services.ec2.model.TagDescription, "value", Some(x: Any)) if obj.getKey == tag => Some("[EDDA_SUPPRESSED]")
    }
    this.addKeyMapper(pf)
  })
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
  * @param ctx context to provide beanMapper and configuration
  */
class AwsAddressCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeAddressesRequest

  override def doCrawl() =
    ctx.awsClient.ec2.describeAddresses(request).getAddresses.asScala.map(
      item => Record(item.getPublicIp, ctx.beanMapper(item))).toSeq
}

/** crawler for AutoScalingGroups
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsAutoScalingGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeAutoScalingGroupsRequest
  request.setMaxRecords(50)

  override def doCrawl() = {
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
      if (Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
        throw new java.lang.RuntimeException("no tags found for any record in " + name + ", ignoring crawl results")
      } else logger.warn("no tags found for any record in " + name + ".  " +
        "If you expect at least one tag then set: edda.crawler." + name + ".abortWithoutTags=true")
    }
    list
  }
}

/** crawler for ASG Policies
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsScalingPolicyCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribePoliciesRequest
  request.setMaxRecords(50)

  override def doCrawl() = {
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

/** crawler for CLoudWatch Alarms
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsAlarmCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = new DescribeAlarmsRequest
  request.setMaxRecords(100)

  override def doCrawl() = {
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
  * @param ctx context to provide beanMapper and configuration
  */
class AwsImageCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeImagesRequest

  override def doCrawl() = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeImages(request).getImages.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getImageId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for LoadBalancers
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsLoadBalancerCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeLoadBalancersRequest

  override def doCrawl() = ctx.awsClient.elb.describeLoadBalancers(request).getLoadBalancerDescriptions.asScala.map(
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
  * @param ctx context to provide beanMapper and configuration
  * @param crawler the LoadBalancer crawler
  */
class AwsInstanceHealthCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler(ctx) {

  import AwsInstanceHealthCrawler._

  override def crawl() {}

  // we don't crawl, just get updates from crawler when it crawls
  override def doCrawl() = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceHealthCrawler")

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  /** for each elb call describeInstanceHealth and map that to a new document
    *
    * @param elbRecords the records to crawl
    * @return the record set for the instanceHealth
    */
  def doCrawl(elbRecords: Seq[Record]): Seq[Record] = {
    val futures: Seq[java.util.concurrent.Future[Record]] = elbRecords.map(
      elb => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val instances = ctx.awsClient.elb.describeInstanceHealth(new DescribeInstanceHealthRequest(elb.id)).getInstanceStates
              elb.copy(data = Map("name" -> elb.id, "instances" -> instances.asScala.map(ctx.beanMapper(_))))
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
            logger.error(this + "exception from describeInstanceHealth", e)
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException("failed to crawl instance health")
    }
    records
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsInstanceHealthCrawlerState()))

  protected override def init() {
    crawler.addObserver(this)
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Crawler.CrawlResult(from, elbRecords), state) => {
      // this is blocking so we dont crawl in parallel
      if (elbRecords ne localState(state).elbRecords) {
        val newRecords = doCrawl(elbRecords)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, newRecords))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceHealthCrawlerState(elbRecords))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}


/** crawler for LaunchConfigurations
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsLaunchConfigurationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeLaunchConfigurationsRequest
  request.setMaxRecords(50)

  override def doCrawl() = {
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
  * @param ctx context to provide beanMapper and configuration
  */
class AwsReservationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeInstancesRequest

  override def doCrawl() = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeInstances(request).getReservations.asScala.map(
      item => {
        tagCount += item.getInstances.asScala.map(_.getTags.size).sum
        Record(item.getReservationId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
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
  * @param ctx context to provide beanMapper and configuration
  * @param crawler the AwsReservation crawler
  */
class AwsInstanceCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler(ctx) {

  import AwsInstanceCrawler._

  override def crawl() {}

  // we dont crawl, just get updates from crawler when it crawls
  override def doCrawl() = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceCrawler")

  def doCrawl(resRecords: Seq[Record]): Seq[Record] = {
    resRecords.flatMap(rec => {
      rec.data.asInstanceOf[Map[String, Any]].get("instances") match {
        case instances: Option[Seq[Map[String, Any]]] => instances.get.map(
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
    crawler.addObserver(this)
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Crawler.CrawlResult(from, reservations), state) => {
      // this is blocking so we dont crawl in parallel
      if (reservations ne localState(state).reservationRecords) {
        val newRecords = doCrawl(reservations)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, newRecords))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceCrawlerState(reservations))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}

/** crawler for Security Group
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsSecurityGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeSecurityGroupsRequest

  override def doCrawl() = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeSecurityGroups(request).getSecurityGroups.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getGroupId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for Snapshots
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsSnapshotCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeSnapshotsRequest

  override def doCrawl() = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeSnapshots(request).getSnapshots.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getSnapshotId, new DateTime(item.getStartTime), ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for all Tags
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsTagCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeTagsRequest

  override def doCrawl() = ctx.awsClient.ec2.describeTags(request).getTags.asScala.map(
    item => Record(item.getKey + "|" + item.getResourceType + "|" + item.getResourceId, ctx.beanMapper(item))).toSeq
}

/** crawler for Volumes
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsVolumeCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeVolumesRequest

  override def doCrawl() = {
    var tagCount = 0
    val list = ctx.awsClient.ec2.describeVolumes(request).getVolumes.asScala.map(
      item => {
        tagCount += item.getTags.size
        Record(item.getVolumeId, new DateTime(item.getCreateTime), ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && Utils.getProperty(ctx.config, "edda.crawler", "abortWithoutTags", name, "false").toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for S3 Buckets
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsBucketCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new ListBucketsRequest

  override def doCrawl() = ctx.awsClient.s3.listBuckets(request).asScala.map(
    item => Record(item.getName, new DateTime(item.getCreationDate), ctx.beanMapper(item))).toSeq
}

/** crawler for SQS Queues
  *
  * This crawler is similar to the InstanceHealth crawler in that it has to first
  * get a list of SQS Queues then for each queue fan-out and query each queue.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsSimpleQueueCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new ListQueuesRequest
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl() = {
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
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            logger.error(this + "exception from SQS getQueueAttributes", e)
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

/** crawler for ReservedInstance (ie pre-paid instances)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsReservedInstanceCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new DescribeReservedInstancesRequest

  override def doCrawl() = ctx.awsClient.ec2.describeReservedInstances(request).getReservedInstances.asScala.map(
    item => Record(item.getReservedInstancesId, new DateTime(item.getStart), ctx.beanMapper(item))).toSeq
}


/** crawler for Route53 Hosted Zones (DNS records)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */ 
class AwsHostedZoneCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler(ctx) {
  val request = new ListHostedZonesRequest

  override def doCrawl() =  ctx.awsClient.route53.listHostedZones(request).getHostedZones.asScala.map(
      item => Record(item.getName, ctx.beanMapper(item))).toSeq
}

case class AwsHostedRecordCrawlerState(hostedZones: Seq[Record] = Seq[Record]())

object AwsHostedRecordCrawler extends StateMachine.LocalState[AwsHostedRecordCrawlerState]

/** crawler for Route53 Resource Record Sets (DNS records)
  *  this is a secondary crawler that crawls the resource recordsets for each hosted zone
  * and then pulls out each recordset in the zones to track them seperately
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  * @param crawler the awsHostedZone crawler
  */ 
class AwsHostedRecordCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler(ctx) {

  import AwsHostedRecordCrawler._

  override def crawl() {}

  // we dont crawl, just get updates from crawler when it crawls
  override def doCrawl() = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on HostedRecordCrawler")
  
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)
  /** for each zone call listResourceRecordSets and map that to a new document
    *
    * @param zones the records to crawl
    * @return the record set for the resourceRecordSet
    */
  def doCrawl(zones: Seq[Record]): Seq[Record] = {
    
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
            logger.error(this + "exception from listResourceRecordSets", e)
            None
          }
        }
      }
    ).collect({
      case Some(rec) => rec
    }).flatten

    if (failed) {
      throw new java.lang.RuntimeException("failed to crawl resource record sets")
    }
    records
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsHostedRecordCrawlerState()))

  protected override def init() {
    crawler.addObserver(this)
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Crawler.CrawlResult(from, hostedZones), state) => {
      // this is blocking so we dont crawl in parallel
      if (hostedZones ne localState(state).hostedZones) {
        val newRecords = doCrawl(hostedZones)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, newRecords))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsHostedRecordCrawlerState(hostedZones))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}
