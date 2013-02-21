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
import com.netflix.edda.GroupCollection
import com.netflix.edda.RootCollection
import com.netflix.edda.Elector
import com.netflix.edda.Record
import com.netflix.edda.Utils

import com.netflix.edda.DataStore
import com.netflix.edda.BeanMapper


import org.slf4j.LoggerFactory

/** Factory to build all known AWS collections
  */
object AwsCollectionBuilder {
  /** builder routine to build collections
    *
    * CollectionManager.register is called for each generated collection object
    *
    * @param ctx a common collection context to be used by all collections
    * @param clientFactory  function that takes the account and region strings and returns an AwsClient object to be used by all collections
    * @param bm beanMapper to be used by all the AWS crawlers
    * @param elector elector to be used for leadership selection
    * @param dsFactory function to return a dataStore for each collection
    */
  def buildAll(ctx: Collection.Context, clientFactory: (String, String) => AwsClient, bm: BeanMapper, elector: Elector, dsFactory: String => Option[DataStore]) {
    val collMap = Option(ctx.config.getProperty("edda.accounts")) match {
      case Some(accountString) => {
        val accounts = accountString.split(",")
        val accountContexts = accounts.map(
          account => account -> new AwsCollection.Context {
            val config = ctx.config
            val beanMapper = bm
            val recordMatcher = ctx.recordMatcher
            val awsClient = clientFactory(account, Utils.getProperty(config, "edda", "region", account, null))
          }).toMap

        // this give us:
        // Map[String,Array[com.netflix.edda.Collection]]
        val accountCollections = accounts.flatMap(
          account => {
            mkCollections(accountContexts(account), account, elector, dsFactory).map(
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
      case None => {
        val context = new AwsCollection.Context {
          val config = ctx.config
          val beanMapper = bm
          val recordMatcher = ctx.recordMatcher
          val awsClient = clientFactory("", config.getProperty("edda.region", null))
        }
        mkCollections(context, "", elector, dsFactory).map(
          collection => collection.rootName -> collection).toMap
      }
    }

    collMap.foreach(pair => CollectionManager.register(pair._1, pair._2))
  }

  /** called by *buildAll* for each account listed in the context config to
    * generate the Collection objects
    */
  def mkCollections(ctx: AwsCollection.Context, accountName: String, elector: Elector, dsFactory: String => Option[DataStore]): Seq[RootCollection] = {
    val res = new AwsReservationCollection(dsFactory, accountName, elector, ctx)
    val elb = new AwsLoadBalancerCollection(dsFactory, accountName, elector, ctx)
    val asg = new AwsAutoScalingGroupCollection(dsFactory, accountName, elector, ctx)
    val inst = new AwsInstanceCollection(res.crawler, dsFactory, accountName, elector, ctx)
    val hostedZones = new AwsHostedZoneCollection(dsFactory, accountName, elector, ctx)
    val hostedRecords = new AwsHostedRecordCollection(hostedZones.crawler, dsFactory, accountName, elector, ctx)
    Seq(
      new AwsAddressCollection(dsFactory, accountName, elector, ctx),
      asg,
      new AwsScalingPolicyCollection(dsFactory, accountName, elector, ctx),
      new AwsAlarmCollection(dsFactory, accountName, elector, ctx),
      new AwsImageCollection(dsFactory, accountName, elector, ctx),
      elb,
      new AwsInstanceHealthCollection(elb.crawler, dsFactory, accountName, elector, ctx),
      new AwsLaunchConfigurationCollection(dsFactory, accountName, elector, ctx),
      res,
      inst,
      new AwsSecurityGroupCollection(dsFactory, accountName, elector, ctx),
      new AwsSnapshotCollection(dsFactory, accountName, elector, ctx),
      new AwsTagCollection(dsFactory, accountName, elector, ctx),
      new AwsVolumeCollection(dsFactory, accountName, elector, ctx),
      new AwsBucketCollection(dsFactory, accountName, elector, ctx),
      new AwsSimpleQueueCollection(dsFactory, accountName, elector, ctx),
      new AwsReservedInstanceCollection(dsFactory, accountName, elector, ctx),
      new GroupAutoScalingGroups(asg, inst, dsFactory, elector, ctx),
      hostedZones,
      hostedRecords
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
  def makeGroupInstances(asgRec: Record, instanceMap: Map[String, Record]): Seq[Map[String, Any]] = {
    val instances = asgRec.data.asInstanceOf[Map[String, Any]]("instances").asInstanceOf[List[Map[String, Any]]]
    val newInstances = instances.filter(
      inst => {
        val id = inst("instanceId").asInstanceOf[String]
        val bool = instanceMap.contains(id)
        if (!bool) {
          logger.warn("asg: " + asgRec.id + " contains unknown instance: " + id)
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
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsAddressCollection(
                            dsFactory: String => Option[DataStore],
                            val accountName: String,
                            val elector: Elector,
                            override val ctx: AwsCollection.Context) extends RootCollection("aws.addresses", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsAddressCrawler(name, ctx)
}

/** collection for AWS AutoScalingGroups
  *
  * root collection name: aws.autoScalingGroups
  *
  * see crawler details [[com.netflix.edda.aws.AwsAutoScalingGroupCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsAutoScalingGroupCollection(
                                     dsFactory: String => Option[DataStore],
                                     val accountName: String,
                                     val elector: Elector,
                                     override val ctx: AwsCollection.Context) extends RootCollection("aws.autoScalingGroups", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsAutoScalingGroupCrawler(name, ctx)
}

/** collection for AWS ASG Scaling Policies
  *
  * root collection name: aws.scalingPolicies
  *
  * see crawler details [[com.netflix.edda.aws.AwsScalingPolicyCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsScalingPolicyCollection(
                                     dsFactory: String => Option[DataStore],
                                     val accountName: String,
                                     val elector: Elector,
                                     override val ctx: AwsCollection.Context) extends RootCollection("aws.scalingPolicies", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsScalingPolicyCrawler(name, ctx)
}

/** collection for AWS ASG Alarms
  *
  * root collection name: aws.alarms
  *
  * see crawler details [[com.netflix.edda.aws.AwsAlarmCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsAlarmCollection(
                                     dsFactory: String => Option[DataStore],
                                     val accountName: String,
                                     val elector: Elector,
                                     override val ctx: AwsCollection.Context) extends RootCollection("aws.alarms", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsAlarmCrawler(name, ctx)
}

/** collection for AWS Images
  *
  * root collection name: aws.images
  *
  * see crawler details [[com.netflix.edda.aws.AwsImageCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsImageCollection(
                          dsFactory: String => Option[DataStore],
                          val accountName: String,
                          val elector: Elector,
                          override val ctx: AwsCollection.Context) extends RootCollection("aws.images", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsImageCrawler(name, ctx)
}

/** collection for AWS Elastic Load Balancers
  *
  * root collection name: aws.loadBalancers
  *
  * see crawler details [[com.netflix.edda.aws.AwsLoadBalancerCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsLoadBalancerCollection(
                                 dsFactory: String => Option[DataStore],
                                 val accountName: String,
                                 val elector: Elector,
                                 override val ctx: AwsCollection.Context) extends RootCollection("aws.loadBalancers", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsLoadBalancerCrawler(name, ctx)
}

/** collection for AWS Elastic Load Balancer Instances
  *
  * root collection name: view.loadBalancerInstances
  *
  * see crawler details [[com.netflix.edda.aws.AwsInstanceHealthCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsInstanceHealthCollection(
                                   val elbCrawler: AwsLoadBalancerCrawler,
                                   dsFactory: String => Option[DataStore],
                                   val accountName: String,
                                   val elector: Elector,
                                   override val ctx: AwsCollection.Context) extends RootCollection("view.loadBalancerInstances", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsInstanceHealthCrawler(name, ctx, elbCrawler)
}

/** collection for AWS LaunchConfigurations
  *
  * root collection name: aws.launchConfigurations
  *
  * see crawler details [[com.netflix.edda.aws.AwsLaunchConfigurationCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsLaunchConfigurationCollection(
                                        dsFactory: String => Option[DataStore],
                                        val accountName: String,
                                        val elector: Elector,
                                        override val ctx: AwsCollection.Context) extends RootCollection("aws.launchConfigurations", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsLaunchConfigurationCrawler(name, ctx)
}

/** collection for AWS Reservations of EC2 Instances
  *
  * root collection name: aws.instances
  *
  * see crawler details [[com.netflix.edda.aws.AwsReservationCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsReservationCollection(
                                dsFactory: String => Option[DataStore],
                                val accountName: String,
                                val elector: Elector,
                                override val ctx: AwsCollection.Context) extends RootCollection("aws.instances", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsReservationCrawler(name, ctx)
}

/** collection for AWS Instances
  *
  * root collection name: view.instances
  *
  * see crawler details [[com.netflix.edda.aws.AwsInstanceCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsInstanceCollection(
                             val resCrawler: AwsReservationCrawler,
                             dsFactory: String => Option[DataStore],
                             val accountName: String,
                             val elector: Elector,
                             override val ctx: AwsCollection.Context) extends RootCollection("view.instances", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsInstanceCrawler(name, ctx, resCrawler)
}

/** collection for AWS Security Groups
  *
  * root collection name: aws.securityGroups
  *
  * see crawler details [[com.netflix.edda.aws.AwsSecurityGroupCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsSecurityGroupCollection(
                                  dsFactory: String => Option[DataStore],
                                  val accountName: String,
                                  val elector: Elector,
                                  override val ctx: AwsCollection.Context) extends RootCollection("aws.securityGroups", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsSecurityGroupCrawler(name, ctx)
}

/** collection for AWS Snapshots
  *
  * root collection name: aws.snapshots
  *
  * see crawler details [[com.netflix.edda.aws.AwsSnapshotCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsSnapshotCollection(
                             dsFactory: String => Option[DataStore],
                             val accountName: String,
                             val elector: Elector,
                             override val ctx: AwsCollection.Context) extends RootCollection("aws.snapshots", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsSnapshotCrawler(name, ctx)
}

/** collection for AWS Tags
  *
  * root collection name: aws.tags
  *
  * see crawler details [[com.netflix.edda.aws.AwsTagCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsTagCollection(
                        dsFactory: String => Option[DataStore],
                        val accountName: String,
                        val elector: Elector,
                        override val ctx: AwsCollection.Context) extends RootCollection("aws.tags", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsTagCrawler(name, ctx)
}

/** collection for AWS Volumes
  *
  * root collection name: aws.volumes
  *
  * see crawler details [[com.netflix.edda.aws.AwsVolumeCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsVolumeCollection(
                           dsFactory: String => Option[DataStore],
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.volumes", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsVolumeCrawler(name, ctx)
}

/** collection for AWS S3 Buckets
  *
  * root collection name: aws.buckets
  *
  * see crawler details [[com.netflix.edda.aws.AwsBucketCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsBucketCollection(
                           dsFactory: String => Option[DataStore],
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.buckets", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsBucketCrawler(name, ctx)
}

/** collection for AWS Simple Queue (SQS)
  *
  * root collection name: view.simpleQueues
  *
  * see crawler details [[com.netflix.edda.aws.AwsSimpleQueueCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsSimpleQueueCollection(
                                dsFactory: String => Option[DataStore],
                                val accountName: String,
                                val elector: Elector,
                                override val ctx: AwsCollection.Context) extends RootCollection("view.simpleQueues", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsSimpleQueueCrawler(name, ctx)

  /** this is overriden from [[com.netflix.edda.aws.Collection]] because there are several
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
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsReservedInstanceCollection(
                                 dsFactory: String => Option[DataStore],
                                 val accountName: String,
                                 val elector: Elector,
                                 override val ctx: AwsCollection.Context) extends RootCollection("aws.reservedInstances", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
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
  * @param dsFactory function that creates new DataStore object from collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class GroupAutoScalingGroups(
                              val asgCollection: AwsAutoScalingGroupCollection,
                              val instanceCollection: AwsInstanceCollection,
                              dsFactory: String => Option[DataStore],
                              val elector: Elector,
                              override val ctx: AwsCollection.Context) extends RootCollection("group.autoScalingGroups", asgCollection.accountName, ctx) with GroupCollection {
  val crawler = asgCollection.crawler
  val dataStore: Option[DataStore] = dsFactory(name)

  // used in GroupCollection
  val mergeKeys = Map("instances" -> "instanceId")

  val instanceQuery = Map(
    "data.state.name" -> Map("$nin" -> Seq("pending", "shutting-down", "terminating", "terminated")))

  /** The crawler is really a [[com.netflix.edda.aws.AwsAutoScalingGroupCrawler]] so we have to
    * translate the ASG records into what the group.autoScalingGroup records should look like
    *
    * @param newRecords the new ASG records from the crawler
    * @param oldRecords these are the previous generation of group.autoScalingGroup records
    * @return the [[com.netflix.edda.Collection.Delta]] modified from [[com.netflix.edda.GroupCollection.groupDelta]]
    */
  override protected def delta(newRecords: Seq[Record], oldRecords: Seq[Record]) = {
    // newRecords will be from the ASG crawler, we need to convert it
    // to the Group records then call groupDelta

    val slotMap = groupSlots(oldRecords)

    val instanceMap = instanceCollection.query(instanceQuery).map(rec => rec.id -> rec).toMap

    val oldMap = oldRecords.map(rec => rec.id -> rec).toMap

    val modNewRecords = newRecords.map(
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
    super.delta(modNewRecords, oldRecords)
  }
}

/** collection for AWS Route53 hosted zones
  *
  * root collection name: aws.hostedZones
  *
  * see crawler details [[com.netflix.edda.aws.AwsHostedZoneCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsHostedZoneCollection(
                           dsFactory: String => Option[DataStore],
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.hostedZones", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsHostedZoneCrawler(name, ctx)
}

/** collection for AWS Route53 record sets
  *
  * root collection name: aws.hostedRecords
  *
  * see crawler details [[com.netflix.edda.aws.AwsHostedRecordCrawler]]
  *
  * @param dsFactory function that creates new DataStore object from collection name
  * @param accountName account name to be prefixed to collection name
  * @param elector Elector to determine leadership
  * @param ctx context for configuration and AWS clients objects
  */
class AwsHostedRecordCollection(
                           val zoneCrawler: AwsHostedZoneCrawler,
                           dsFactory: String => Option[DataStore],
                           val accountName: String,
                           val elector: Elector,
                           override val ctx: AwsCollection.Context) extends RootCollection("aws.hostedRecords", accountName, ctx) {
  val dataStore: Option[DataStore] = dsFactory(name)
  val crawler = new AwsHostedRecordCrawler(name, ctx, zoneCrawler)
}
