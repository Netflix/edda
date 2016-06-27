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


import com.netflix.edda.Collection
import com.netflix.edda.CollectionManager
import com.netflix.edda.MergedCollection
import com.netflix.edda.Queryable
import com.netflix.edda.GroupCollection
import com.netflix.edda.RootCollection
import com.netflix.edda.Elector
import com.netflix.edda.Record
import com.netflix.edda.RecordSet
import com.netflix.edda.Utils
import com.netflix.edda.RequestId

import com.netflix.edda.Datastore
import com.netflix.edda.BeanMapper

import org.joda.time.DateTime

import org.slf4j.LoggerFactory

/** Factory to build all known AWS collections
  */
object AwsCollectionBuilder {
  /** builder routine to build collections
    *
    * CollectionManager.register is called for each generated collection object
    *
    * @param ctx a common collection context to be used by all collections
    * @param clientFactory  function that takes the account string and returns an AwsClient object to be used by all collections
    * @param bm beanMapper to be used by all the AWS crawlers
    * @param elector elector to be used for leadership selection
    */
  def buildAll(ctx: Collection.Context, clientFactory: (String) => AwsClient, bm: BeanMapper, elector: Elector) {
    val collMap = Utils.getProperty("edda","accounts","","").get match {
      case "" => {
        val context = new AwsCollection.Context {
          val beanMapper = bm
          val recordMatcher = ctx.recordMatcher
          val awsClient = clientFactory("")
        }
        mkCollections(context, "", elector).map(
          collection => collection.rootName -> collection).toMap
      }
      case accountString => {
        val accounts = accountString.split(",")
        val accountContexts = accounts.map(
          account => account -> new AwsCollection.Context {
            val beanMapper = bm
            val recordMatcher = ctx.recordMatcher
            val awsClient = clientFactory(account)
          }).toMap

        // this give us:
        // Map[String,Array[com.netflix.edda.Collection]]
        val accountCollections = accounts.flatMap(
          account => {
            mkCollections(accountContexts(account), account, elector).map(
              collection => collection.rootName -> collection)
          }).groupBy(_._1).mapValues(c => c.map(x => x._2))
        // now map the Array's to a MergedCollection
        // need to return name -> _ for each array element
        // concat with MergedCollection(Array)
        accountCollections.flatMap(
          pair => {
            val name = pair._1
            val collections = pair._2
            collections.map(coll => coll.name -> coll).toMap ++ Map(name -> new MergedCollection(name, collections))
          })
      }
    }

    collMap.foreach(pair => CollectionManager.register(pair._1, pair._2))
  }

  /** called by *buildAll* for each account listed in the config to
    * generate the Collection objects
    */
  def mkCollections(ctx: AwsCollection.Context, accountName: String, elector: Elector): Seq[RootCollection] = {
    val res = new AwsReservationCollection(accountName, elector, ctx)
    val elb = new AwsLoadBalancerCollection(accountName, elector, ctx)
    val asg = new AwsAutoScalingGroupCollection(accountName, elector, ctx)
    val inst = new AwsInstanceCollection(res.crawler, accountName, elector, ctx)
    val hostedZones = new AwsHostedZoneCollection(accountName, elector, ctx)
    val hostedRecords = new AwsHostedRecordCollection(hostedZones.crawler, accountName, elector, ctx)
    Seq(
      new AwsAddressCollection(accountName, elector, ctx),
      asg,
      new AwsScalingPolicyCollection(accountName, elector, ctx),
      new AwsAlarmCollection(accountName, elector, ctx),
      new AwsImageCollection(accountName, elector, ctx),
      elb,
      new AwsInstanceHealthCollection(elb.crawler, accountName, elector, ctx),
      new AwsLaunchConfigurationCollection(accountName, elector, ctx),
      res,
      inst,
      new AwsSecurityGroupCollection(accountName, elector, ctx),
      new AwsSnapshotCollection(accountName, elector, ctx),
      new AwsTagCollection(accountName, elector, ctx),
      new AwsVolumeCollection(accountName, elector, ctx),
      new AwsBucketCollection(accountName, elector, ctx),
      new AwsIamUserCollection(accountName, elector, ctx),
      new AwsIamGroupCollection(accountName, elector, ctx),
      new AwsIamRoleCollection(accountName, elector, ctx),
      new AwsIamVirtualMFADeviceCollection(accountName, elector, ctx),
      new AwsSimpleQueueCollection(accountName, elector, ctx),
      new AwsReservedInstanceCollection(accountName, elector, ctx),
      new GroupAutoScalingGroups(asg, inst, elector, ctx),
      hostedZones,
      hostedRecords,
      new AwsDatabaseCollection(accountName, elector, ctx),
      new AwsCacheClusterCollection(accountName, elector, ctx),
      new AwsSubnetCollection(accountName, elector, ctx),
      new AwsCloudformationCollection(accountName, elector, ctx),
			new AwsScalingActivitiesCollection(accountName, elector, ctx),
			new AwsScheduledActionsCollection(accountName, elector, ctx),
			new AwsVpcCollection(accountName, elector, ctx),
			new AwsReservedInstancesOfferingCollection(accountName, elector, ctx)
    )
  }
}

/** static routines to be shared for AWS related collections */
object AwsCollection {

  abstract class Context() extends Collection.Context with AwsCrawler.Context

  private[this] val logger = LoggerFactory.getLogger(getClass)

  /** constructs list of Instances to used for the group* collections
    *
    * @param asgRec the ASG Record to get instance details from
    * @param instanceMap the list of know instance records
    * @return a sequence of Maps containing instance details
    */
  def makeGroupInstances(asgRec: Record, instanceMap: Map[String, Record])(implicit req: RequestId): Seq[Map[String, Any]] = {
    val instances = asgRec.data.asInstanceOf[Map[String, Any]]("instances").asInstanceOf[List[Map[String, Any]]]
    val newInstances = instances.filter(
      inst => {
        val id = inst("instanceId").asInstanceOf[String]
        val bool = instanceMap.contains(id)
        if (!bool) {
          if (logger.isWarnEnabled) logger.warn(s"$req asg: ${asgRec.id} contains unknown instance: $id")
        }
        bool
      }).map(asgInst => {
      val id = asgInst("instanceId").asInstanceOf[String]
      val instance = instanceMap(id)
      val instanceData = instance.data.asInstanceOf[Map[String, Any]]
      Map(
        "availabilityZone" -> asgInst("availabilityZone"),
        "imageId" -> instanceData.get("imageId").getOrElse(null),
        "instanceId" -> id,
        "instanceType" -> instanceData.get("instanceType").getOrElse(null),
        "launchTime" -> instance.ctime,
        "lifecycleState" -> asgInst("lifecycleState"),
        "platform" -> instanceData.get("platform").getOrElse(null),
        "privateIpAddress" -> instanceData.get("privateIpAddress").getOrElse(null),
        "publicDnsName" -> instanceData.get("publicDnsName").getOrElse(null),
        "publicIpAddress" -> instanceData.get("publicIpAddress").getOrElse(null),
        "start" -> instance.ctime,
        "vpcId" -> instanceData.get("vpcId").getOrElse(null))
    })
    newInstances
  }
}

/** collection for AWS Addresses
  *
  * root collection name: aws.addresses
  *
  * see crawler details [[com.netflix.edda.aws.AwsAddressCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsAddressCollection(
                            val accountName: String,
                            val elector: Elector,
                            override val ctx: AwsCollection.Context) extends RootCollection("aws.addresses", accountName, ctx) {
  val crawler = new AwsAddressCrawler(name, ctx)
}

/** collection for AWS AutoScalingGroups
  *
  * root collection name: aws.autoScalingGroups
  *
  * see crawler details [[com.netflix.edda.aws.AwsAutoScalingGroupCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsAutoScalingGroupCollection(
                                     val accountName: String,
                                     val elector: Elector,
                                     override val ctx: AwsCollection.Context) extends RootCollection("aws.autoScalingGroups", accountName, ctx) {
  val crawler = new AwsAutoScalingGroupCrawler(name, ctx)
}

/** collection for AWS ASG Scaling Policies
  *
  * root collection name: aws.scalingPolicies
  *
  * see crawler details [[com.netflix.edda.aws.AwsScalingPolicyCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsScalingPolicyCollection(
                                     val accountName: String,
                                     val elector: Elector,
                                     override val ctx: AwsCollection.Context) extends RootCollection("aws.scalingPolicies", accountName, ctx) {
  val crawler = new AwsScalingPolicyCrawler(name, ctx)
}

/** collection for AWS ASG Alarms
  *
  * root collection name: aws.alarms
  *
  * see crawler details [[com.netflix.edda.aws.AwsAlarmCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsAlarmCollection(
                                     val accountName: String,
                                     val elector: Elector,
                                     override val ctx: AwsCollection.Context) extends RootCollection("aws.alarms", accountName, ctx) {
  val crawler = new AwsAlarmCrawler(name, ctx)
}

/** collection for AWS AutoScaling Activities
 *
 * root collection name: aws.scalingActivities
 *
 * see crawler details [[com.netflix.edda.aws.AwsScalingActivitiesCrawler]]
 *
 * @param accountName account name to be prefixed to collection name
 * @param elector Elector to determine leadership
 * @param ctx context for AWS clients objects
 */
 class AwsScalingActivitiesCollection(
                                val accountName: String,
                                val elector: Elector,
                                override val ctx: AwsCollection.Context) extends RootCollection("aws.scalingActivities", accountName, ctx) {
   val crawler = new AwsScalingActivitiesCrawler(name, ctx)
 }

 /** collection for AWS AutoScaling Scheduled Actions
  *
  * root collection name: aws.scheduledActions
  *
  * see crawler details [[com.netflix.edda.aws.AwsScheduledActionsCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
  class AwsScheduledActionsCollection(
                                 val accountName: String,
                                 val elector: Elector,
                                 override val ctx: AwsCollection.Context) extends RootCollection("aws.scheduledActions", accountName, ctx) {
    val crawler = new AwsScheduledActionsCrawler(name, ctx)
  }

/** collection for AWS VPCs
  *
  * root collection name: aws.vpcs
  *
  * see crawler details [[com.netflix.edda.aws.AwsVpcCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
*/
class AwsVpcCollection(
                 val accountName: String,
                 val elector: Elector,
                 override val ctx: AwsCollection.Context) extends RootCollection("aws.vpcs", accountName, ctx) {
                 val crawler = new AwsVpcCrawler(name, ctx)
}

/** collection for AWS Reserved Instance Offerings
  *
  * root collection name: aws.vpcs
  *
  * see crawler details [[com.netflix.edda.aws.AwsReservedInstancesOfferingCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
*/
class AwsReservedInstancesOfferingCollection(
                 val accountName: String,
                 val elector: Elector,
                 override val ctx: AwsCollection.Context) extends RootCollection("aws.reservedInstancesOfferings", accountName, ctx) {
                 val crawler = new AwsReservedInstancesOfferingCrawler(name, ctx)
}

/** collection for AWS Images
  *
  * root collection name: aws.images
  *
  * see crawler details [[com.netflix.edda.aws.AwsImageCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsImageCollection(
                          val accountName: String,
                          val elector: Elector,
                          override val ctx: AwsCollection.Context) extends RootCollection("aws.images", accountName, ctx) {
  val crawler = new AwsImageCrawler(name, ctx)
}

/** collection for AWS Elastic Load Balancers
  *
  * root collection name: aws.loadBalancers
  *
  * see crawler details [[com.netflix.edda.aws.AwsLoadBalancerCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsLoadBalancerCollection(
                                 val accountName: String,
                                 val elector: Elector,
                                 override val ctx: AwsCollection.Context) extends RootCollection("aws.loadBalancers", accountName, ctx) {
  val crawler = new AwsLoadBalancerCrawler(name, ctx)
}

/** collection for AWS Elastic Load Balancer Instances
  *
  * root collection name: view.loadBalancerInstances
  *
  * see crawler details [[com.netflix.edda.aws.AwsInstanceHealthCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsInstanceHealthCollection(
                                   val elbCrawler: AwsLoadBalancerCrawler,
                                   val accountName: String,
                                   val elector: Elector,
                                   override val ctx: AwsCollection.Context) extends RootCollection("view.loadBalancerInstances", accountName, ctx) {
  // we dont actually crawl, the elbCrawler triggers our crawl events
  override val allowCrawl = false
  val crawler = new AwsInstanceHealthCrawler(name, ctx, elbCrawler)
}

/** collection for AWS LaunchConfigurations
  *
  * root collection name: aws.launchConfigurations
  *
  * see crawler details [[com.netflix.edda.aws.AwsLaunchConfigurationCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsLaunchConfigurationCollection(
                                        val accountName: String,
                                        val elector: Elector,
                                        override val ctx: AwsCollection.Context) extends RootCollection("aws.launchConfigurations", accountName, ctx) {
  val crawler = new AwsLaunchConfigurationCrawler(name, ctx)
}

/** collection for AWS Reservations of EC2 Instances
  *
  * root collection name: aws.instances
  *
  * see crawler details [[com.netflix.edda.aws.AwsReservationCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsReservationCollection(
                                val accountName: String,
                                val elector: Elector,
                                override val ctx: AwsCollection.Context) extends RootCollection("aws.instances", accountName, ctx) {
  val crawler = new AwsReservationCrawler(name, ctx)
}

/** collection for AWS Instances
  *
  * root collection name: view.instances
  *
  * see crawler details [[com.netflix.edda.aws.AwsInstanceCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsInstanceCollection(
                             val resCrawler: AwsReservationCrawler,
                             val accountName: String,
                             val elector: Elector,
                             override val ctx: AwsCollection.Context) extends RootCollection("view.instances", accountName, ctx) {
  // we dont actually crawl, the resCrawler triggers our crawl events
  override val allowCrawl = false
  val crawler = new AwsInstanceCrawler(name, ctx, resCrawler)
}

/** collection for AWS Security Groups
  *
  * root collection name: aws.securityGroups
  *
  * see crawler details [[com.netflix.edda.aws.AwsSecurityGroupCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsSecurityGroupCollection(
                                  val accountName: String,
                                  val elector: Elector,
                                  override val ctx: AwsCollection.Context) extends RootCollection("aws.securityGroups", accountName, ctx) {
  val crawler = new AwsSecurityGroupCrawler(name, ctx)
}

/** collection for AWS Snapshots
  *
  * root collection name: aws.snapshots
  *
  * see crawler details [[com.netflix.edda.aws.AwsSnapshotCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsSnapshotCollection(
                             val accountName: String,
                             val elector: Elector,
                             override val ctx: AwsCollection.Context) extends RootCollection("aws.snapshots", accountName, ctx) {
  val crawler = new AwsSnapshotCrawler(name, ctx)
}

/** collection for AWS Tags
  *
  * root collection name: aws.tags
  *
  * see crawler details [[com.netflix.edda.aws.AwsTagCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsTagCollection(
                        val accountName: String,
                        val elector: Elector,
                        override val ctx: AwsCollection.Context) extends RootCollection("aws.tags", accountName, ctx) {
  val crawler = new AwsTagCrawler(name, ctx)
}

/** collection for AWS Volumes
  *
  * root collection name: aws.volumes
  *
  * see crawler details [[com.netflix.edda.aws.AwsVolumeCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsVolumeCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.volumes", accountName, ctx) {
  val crawler = new AwsVolumeCrawler(name, ctx)
}

/** collection for AWS S3 Buckets
  *
  * root collection name: aws.buckets
  *
  * see crawler details [[com.netflix.edda.aws.AwsBucketCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsBucketCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.buckets", accountName, ctx) {
  val crawler = new AwsBucketCrawler(name, ctx)
}

/** collection for AWS IAM Users
  *
  * root collection name: aws.iamUsers
  *
  * see crawler details [[com.netflix.edda.aws.AwsIamUserCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsIamUserCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.iamUsers", accountName, ctx) {
  val crawler = new AwsIamUserCrawler(name, ctx)
}

/** collection for AWS IAM Groups
  *
  * root collection name: aws.iamGroups
  *
  * see crawler details [[com.netflix.edda.aws.AwsIamGroupCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsIamGroupCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.iamGroups", accountName, ctx) {
  val crawler = new AwsIamGroupCrawler(name, ctx)
}

/** collection for AWS IAM Roles
  *
  * root collection name: aws.iamRoles
  *
  * see crawler details [[com.netflix.edda.aws.AwsIamRoleCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsIamRoleCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.iamRoles", accountName, ctx) {
  val crawler = new AwsIamRoleCrawler(name, ctx)
}

/** collection for AWS IAM VirtualMFADevices
  *
  * root collection name: aws.iamVirtualMFADevices
  *
  * see crawler details [[com.netflix.edda.aws.AwsIamVirtualMFADeviceCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsIamVirtualMFADeviceCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.iamVirtualMFADevices", accountName, ctx) {
  val crawler = new AwsIamVirtualMFADeviceCrawler(name, ctx)
}

/** collection for AWS Simple Queue (SQS)
  *
  * root collection name: view.simpleQueues
  *
  * see crawler details [[com.netflix.edda.aws.AwsSimpleQueueCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsSimpleQueueCollection(
                                val accountName: String,
                                val elector: Elector,
                                override val ctx: AwsCollection.Context) extends RootCollection("view.simpleQueues", accountName, ctx) {
  val crawler = new AwsSimpleQueueCrawler(name, ctx)

  /** this is overriden from com.netflix.edda.aws.Collection because there are several
    * keys like ApproximateNumberOfMessages that are changing constantly.  We want to record
    * those changes, but not create new document revisions if the only changes are Approx* values
    */
  override protected
  def newStateTimeForChange(newRec: Record, oldRec: Record): Boolean = {
    if (newRec == null || oldRec == null) return true
    val newData = newRec.data.asInstanceOf[Map[String, Any]]
    val oldData = oldRec.data.asInstanceOf[Map[String, Any]]
    val newNoApprox = newRec.copy(data = newData + ("attributes" -> newData("attributes").asInstanceOf[Map[String, String]].filterNot(_._1.startsWith("Approx"))))
    val oldNoApprox = oldRec.copy(data = oldData + ("attributes" -> oldData("attributes").asInstanceOf[Map[String, String]].filterNot(_._1.startsWith("Approx"))))
    newRec.data != oldRec.data && newNoApprox.dataString != oldNoApprox.dataString
  }
}

/** collection for AWS Reserved Instances (pre-paid instances)
  *
  * root collection name: aws.reservedInstances
  *
  * see crawler details [[com.netflix.edda.aws.AwsReservedInstanceCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsReservedInstanceCollection(
                                 val accountName: String,
                                 val elector: Elector,
                                 override val ctx: AwsCollection.Context) extends RootCollection("aws.reservedInstances", accountName, ctx) {
  val crawler = new AwsReservedInstanceCrawler(name, ctx)
}


/** collection for abstracted groupings of instances in AutoScalingGroups
  *
  * root collection name: group.autoScalingGroups
  *
  * see crawler details [[com.netflix.edda.aws.AwsAutoScalingGroupCrawler]]
  *
  * @param asgCollection ASG collection where the crawler used comes from
  * @param instanceCollection Instance Collection so we can query for instance details
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class GroupAutoScalingGroups(
                              val asgCollection: AwsAutoScalingGroupCollection,
                              val instanceCollection: AwsInstanceCollection,
                              val elector: Elector,
                              override val ctx: AwsCollection.Context) extends RootCollection("group.autoScalingGroups", asgCollection.accountName, ctx) with GroupCollection {
  import Utils._
  import Queryable._
  // dont crawl, we get crawl results from the asgCollection crawler
  override val allowCrawl = false
  val crawler = asgCollection.crawler

  // used in GroupCollection
  val mergeKeys = Map("instances" -> "instanceId")

  val instanceQuery = Map(
    "data.state.name" -> Map("$nin" -> Seq("pending", "shutting-down", "terminating", "terminated")))

  /** The crawler is really a com.netflix.edda.aws.AwsAutoScalingGroupCrawler so we have to
    * translate the ASG records into what the group.autoScalingGroup records should look like
    *
    * @param newRecords the new ASG records from the crawler
    * @param oldRecords these are the previous generation of group.autoScalingGroup records
    * @return the [[com.netflix.edda.Collection.Delta]] modified from com.netflix.edda.GroupCollection.groupDelta
    */
  override protected[edda] def delta(newRecordSet: RecordSet, oldRecordSet: RecordSet)(implicit req: RequestId): Collection.Delta = {
    // newRecords will be from the ASG crawler, we need to convert it
    // to the Group records then call groupDelta

    val slotMap = groupSlots(oldRecordSet.records)
    val records = try {
      scala.concurrent.Await.result(
        instanceCollection.query(instanceQuery),
        scala.concurrent.duration.Duration(
          60000,
          scala.concurrent.duration.MILLISECONDS
        )
      )
    } catch {
      case e: Throwable => {
        logger.error(s"failed to fetch instances for $name delta: $e")
        throw e
      }
    }
    val instanceMap = records.map(rec => rec.id -> rec).toMap
    val oldMap = oldRecordSet.records.map(rec => rec.id -> rec).toMap

    val modNewRecords = newRecordSet.records.map(
      asgRec => {
        val newInstances = assignSlots(
          AwsCollection.makeGroupInstances(asgRec, instanceMap),
          "instanceId",
          slotMap("instances"))
        val asgData = asgRec.data.asInstanceOf[Map[String, Any]]

        val ctime = oldMap.get(asgRec.id) match {
          case Some(rec) => rec.ctime
          case None => asgRec.ctime
        }

        val data = Map(
          "desiredCapacity" -> asgData.get("desiredCapacity").getOrElse(null),
          "instances" -> newInstances,
          "launchConfigurationName" -> asgData.get("launchConfigurationName").getOrElse(null),
          "loadBalancerNames" -> asgData.get("loadBalancerNames").getOrElse(List()),
          "maxSize" -> asgData.get("maxSize").getOrElse(null),
          "minSize" -> asgData.get("minSize").getOrElse(null),
          "name" -> asgRec.id,
          "start" -> ctime)

        asgRec.copy(data = data)
      })
    super.delta(RecordSet(modNewRecords,newRecordSet.meta), oldRecordSet)
  }

  override protected[edda] def update(d: Collection.Delta)(implicit req: RequestId): Collection.Delta = {
    if (dataStore.isDefined && (d.changed.size > 0 || d.added.size > 0 || d.removed.size > 0) ) {
      // make sure slots are not reassigned
      // get SET of "old" instances
      val oldInstances = d.changed.flatMap( update => {
        val instances = update.oldRecord.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[Seq[Map[String,Any]]]
        instances.map( instance => instance("instanceId").asInstanceOf[String])
      }).toSet

      // get MAP of "new" instances -> slot (new updates + added records)
      val newInstances = (d.changed.map(update => update.newRecord) ++ d.added).flatMap( rec => {
        val instances = rec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[Seq[Map[String,Any]]]
        instances.map( instance => instance("instanceId").asInstanceOf[String] -> instance("slot").asInstanceOf[Int])
      }).toMap

      // just get a list of the instances that we think are new (ie have had new slot assigned)
      val addedInstances = newInstances.keySet diff oldInstances

      if( addedInstances.size > 0 ) {
        // query db for all "new" instances to get slot
        val query = Map(
          "data.instances.instanceId" -> Map("$in" -> addedInstances.toSeq),
          "$or" -> List(
            Map("ltime" -> null),
            Map("ltime" -> Map("$gt" -> DateTime.now.minusDays(2)))
          )
        )

        val recs = dataStore.get.query(query, limit=0, keys=Set("data.instances.instanceId", "data.instances.slot", "stime"), replicaOk=false)

        recs.foreach( rec => {
          val instances = rec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[Seq[Map[String,Any]]]
          instances.foreach( instance => {
            val id = instance("instanceId").asInstanceOf[String]
            if( newInstances.contains(id) ) {
              val slot = instance("slot").asInstanceOf[Int]
              if( newInstances(id) != slot ) {
                val msg = this.toString + " Slot reassignment for instance " + id + " from " + slot + " [stime=" + rec.stime.getMillis + "] to " + newInstances(id)
                logger.error(s"$req $msg")
                if( ! Utils.getProperty("edda","collection.allowSlotReassign",name,"false").get.toBoolean ) {
                  throw new java.lang.RuntimeException(msg)
                }
              }
            }
          })
        })
      }
    }
    super.update(d)
  }
}

/** collection for AWS Route53 hosted zones
  *
  * root collection name: aws.hostedZones
  *
  * see crawler details [[com.netflix.edda.aws.AwsHostedZoneCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsHostedZoneCollection(
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.hostedZones", accountName, ctx) {
  val crawler = new AwsHostedZoneCrawler(name, ctx)
}

/** collection for AWS Route53 record sets
  *
  * root collection name: aws.hostedRecords
  *
  * see crawler details [[com.netflix.edda.aws.AwsHostedRecordCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsHostedRecordCollection(
                           val zoneCrawler: AwsHostedZoneCrawler,
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.hostedRecords", accountName, ctx) {
  // we dont actually crawl, the zoneCrawler triggers our crawl events
  override val allowCrawl = false
  val crawler = new AwsHostedRecordCrawler(name, ctx, zoneCrawler)
}

/** collection for AWS RDS database instances
  *
  * root collection name: aws.databases
  *
  * see crawler details [[com.netflix.edda.aws.AwsDatabaseCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsDatabaseCollection(
                               val accountName: String,
                               val elector: Elector,
                               override val ctx: AwsCollection.Context) extends RootCollection("aws.databases", accountName, ctx) {
  val crawler = new AwsDatabaseCrawler(name, ctx)

  /** this is overriden from com.netflix.edda.aws.Collection because we want to record
    * changes, but not create new document revisions if the only changes are to latestRestorableTime values
    */
  override protected
  def newStateTimeForChange(newRec: Record, oldRec: Record): Boolean = {
    if (newRec == null || oldRec == null) return true
    val newData = newRec.data.asInstanceOf[Map[String, Any]]
    val oldData = oldRec.data.asInstanceOf[Map[String, Any]]
    val newLatestRestorable = newRec.copy(data = newData.filterNot(_._1.startsWith("latestRestorable")))
    val oldLatestRestorable = oldRec.copy(data = oldData.filterNot(_._1.startsWith("latestRestorable")))
    newRec.data != oldRec.data && newLatestRestorable.dataString != oldLatestRestorable.dataString
  }
}

/** collection for AWS ElastiCache clusters
  *
  * root collection name: aws.cacheClusters
  *
  * see crawler details [[com.netflix.edda.aws.AwsCacheClusterCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsCacheClusterCollection(
                                val accountName: String,
                                val elector: Elector,
                                override val ctx: AwsCollection.Context) extends RootCollection("aws.cacheClusters", accountName, ctx) {
  val crawler = new AwsCacheClusterCrawler(name, ctx)
}

/** collection for AWS Subnets
  *
  * root collection name: aws.subnets
  *
  * see crawler details [[com.netflix.edda.aws.AwsSnapshotCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsSubnetCollection(
                             val accountName: String,
                             val elector: Elector,
                             override val ctx: AwsCollection.Context) extends RootCollection("aws.subnets", accountName, ctx) {
  val crawler = new AwsSubnetCrawler(name, ctx)
}
/** collection for AWS Cloudformation Stacks
  *
  * root collection name: aws.stacks
  *
  * see crawler details [[com.netflix.edda.aws.AwsCloudformationCrawler]]
  *
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for AWS clients objects
  */
class AwsCloudformationCollection(
                               val accountName: String,
                               val elector: Elector,
                               override val ctx: AwsCollection.Context) extends RootCollection("aws.stacks", accountName, ctx) {
  val crawler = new AwsCloudformationCrawler(name, ctx)
}
