package com.netflix.edda.aws

import com.netflix.edda.Collection
import com.netflix.edda.CollectionManager
import com.netflix.edda.MergedCollection
import com.netflix.edda.GroupCollection
import com.netflix.edda.RootCollection
import com.netflix.edda.Crawler
import com.netflix.edda.Elector
import com.netflix.edda.Queryable
import com.netflix.edda.Record
import com.netflix.edda.StateMachine
import com.netflix.edda.Utils

import com.netflix.edda.Datastore
import com.netflix.edda.BeanMapper

import com.amazonaws.auth.AWSCredentials

import org.slf4j.{Logger, LoggerFactory}

import org.joda.time.DateTime

object AwsCollectionBuilder {
    def buildAll(ctx: Collection.Context, clientFactory: String => AwsClient, bm: BeanMapper, elector: Elector, dsFactory: String => Option[Datastore]) {
        val collMap = Option(ctx.config.getProperty("edda.accounts")) match {
            case Some(accountString) => {
                val accounts = accountString.split(",");
                val accountContexts = accounts.map(
                    account => account -> new AwsCollection.Context {
                        val config = ctx.config
                        val beanMapper = bm
                        val recordMatcher = ctx.recordMatcher
                        val awsClient = clientFactory(Utils.getProperty(config, "edda", "region", account, null))
                    }
                ).toMap
                
                // this give us:
                // Map[String,Array[com.netflix.edda.Collection]]
                val accountCollections = accounts.flatMap(
                    account => {
                        mkCollections(accountContexts(account), account, elector, dsFactory).map(
                            collection => collection.rootName -> collection
                        )
                    }
                ).groupBy(_._1).mapValues(c => c.map(x => x._2))
                // now map the Array's to a MergedCollection
                // need to return name -> _ for each array element
                // concat with MergedCollection(Array)
                accountCollections.flatMap(
                    pair => {
                        val name = pair._1
                        val collections = pair._2
                        collections.map( coll => coll.name -> coll ).toMap ++ Map(name -> new MergedCollection(name, collections))
                    }
                )
            }
            case None => {
                val context = new AwsCollection.Context {
                    val config = ctx.config
                    val beanMapper = bm
                    val recordMatcher = ctx.recordMatcher
                    val awsClient = clientFactory(config.getProperty("edda.region", null))
                }
                mkCollections(context, "", elector, dsFactory).map( 
                    collection => collection.rootName -> collection
                ).toMap
            }
        }

        collMap.foreach( pair => CollectionManager.register(pair._1,pair._2) )
    }
        
    def mkCollections(  ctx: AwsCollection.Context, accountName: String, elector: Elector, dsFactory: String => Option[Datastore]): Seq[RootCollection] = {
        val res = new AwsReservationCollection(dsFactory, accountName, elector, ctx)
        val elb = new AwsLoadBalancerCollection(dsFactory, accountName, elector, ctx)
        val asg = new AwsAutoScalingGroupCollection(dsFactory, accountName, elector, ctx)
        val inst = new AwsInstanceCollection(res.crawler, dsFactory, accountName, elector, ctx)
        return Seq(
            new AwsAddressCollection(dsFactory, accountName, elector, ctx),
            asg,
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
            new GroupAutoScalingGroups(asg, inst, dsFactory, elector, ctx)
        )
    }
}

object AwsCollection {
    abstract class Context() extends Collection.Context with AwsCrawler.Context;

    private[this] val logger = LoggerFactory.getLogger(getClass)
    // used for the group collections

    def makeGroupInstances(asgRec: Record, instanceMap: Map[String,Record]): Seq[Map[String,Any]] = {
        val instances = asgRec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[List[Map[String,Any]]]
        val newInstances = instances.filter(
            inst => {
                val id = inst("instanceId").asInstanceOf[String]
                val bool = instanceMap.contains(id)
                if( !bool ) {
                    logger.warn("asg: " + asgRec.id + " contains unknown instance: " + id)
                }
                bool
            }
        ).map(asgInst => { 
            val id = asgInst("instanceId").asInstanceOf[String]
            val instance = instanceMap(id);
            val instanceData = instance.data.asInstanceOf[Map[String,Any]]
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
                "start" -> instance.ctime
            )
        })
        newInstances
    }
}

class AwsAddressCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.addresses", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsAddressCrawler(name, ctx)
}

class AwsAutoScalingGroupCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.autoScalingGroups", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsAutoScalingGroupCrawler(name, ctx)
}

class AwsImageCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.images", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsImageCrawler(name, ctx)
}

class AwsLoadBalancerCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.loadBalancers", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsLoadBalancerCrawler(name, ctx)
}

class AwsInstanceHealthCollection(
    val elbCrawler: AwsLoadBalancerCrawler,
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("view.loadBalancerInstances", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsInstanceHealthCrawler(name, ctx, elbCrawler)
}

class AwsLaunchConfigurationCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.launchConfigurations", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsLaunchConfigurationCrawler(name, ctx)
}

class AwsReservationCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.instances", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsReservationCrawler(name, ctx)
}

class AwsInstanceCollection(
    val resCrawler: AwsReservationCrawler,
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("view.instances", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsInstanceCrawler(name, ctx, resCrawler)
}

class AwsSecurityGroupCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.securityGroups", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsSecurityGroupCrawler(name, ctx)
}

class AwsSnapshotCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.snapshots", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsSnapshotCrawler(name, ctx)
}

class AwsTagCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.tags", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsTagCrawler(name, ctx)
}

class AwsVolumeCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.volumes", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsVolumeCrawler(name, ctx)
}

class AwsBucketCollection(
    dsFactory: String => Option[Datastore],
    val accountName: String,
    val elector: Elector,
    override val ctx : AwsCollection.Context
) extends RootCollection("aws.buckets", accountName, ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsBucketCrawler(name, ctx)
}

class GroupAutoScalingGroups(
    val asgCollection: AwsAutoScalingGroupCollection,
    val instanceCollection: AwsInstanceCollection,
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    override val ctx: AwsCollection.Context
) extends RootCollection("group.autoScalingGroups", asgCollection.accountName, ctx) with GroupCollection {
    val crawler = asgCollection.crawler
    val datastore: Option[Datastore] = dsFactory(name)

    private[this] val logger = LoggerFactory.getLogger(getClass)

    // used in GroupCollection
    val mergeKeys = Map("instances" -> "instanceId")

    override protected
    def delta(newRecords: Seq[Record], oldRecords: Seq[Record]) =  {
        // newRecords will be from the ASG crawler, we need to convert it
        // to the Group records then call groupDelta

        val slotMap = groupSlots(oldRecords)

        val instanceMap = instanceCollection.query(Map.empty).map(rec => rec.id -> rec).toMap

        val oldMap = oldRecords.map( rec => rec.id -> rec).toMap

        val modNewRecords = newRecords.map(
            asgRec => {
                val newInstances = assignSlots(
                    AwsCollection.makeGroupInstances(asgRec, instanceMap),
                    "instanceId",
                    slotMap("instances")
                )
                val asgData = asgRec.data.asInstanceOf[Map[String,Any]]

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
                    "start" -> ctime
                )
                
                asgRec.copy(data=data)
            }
        )
        groupDelta(modNewRecords,oldRecords)
    }
}
