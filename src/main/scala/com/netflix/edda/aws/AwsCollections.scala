package com.netflix.edda.aws

import com.netflix.edda.Collection
import com.netflix.edda.MergedCollection
import com.netflix.edda.GroupCollection
import com.netflix.edda.Crawler
import com.netflix.edda.Elector
import com.netflix.edda.Queryable
import com.netflix.edda.Record
import com.netflix.edda.StateMachine

import com.netflix.edda.Datastore
import com.netflix.edda.BeanMapper

import org.slf4j.{Logger, LoggerFactory}

import org.joda.time.DateTime

object AwsCollectionBuilder {
    def buildAll(ctx: Collection.Context, bm: BeanMapper, elector: Elector, dsFactory: String => Option[Datastore]) {
        val accounts = ctx.config.getProperty("edda.accounts","").split(",");
        val accountContexts = accounts.map(
            account => account -> new AwsCollection.Context {
                val config = ctx.config
                val beanMapper = bm
                val recordMatcher = ctx.recordMatcher
                val awsClient = new AwsClient(
                    account,
                    config.getProperty("edda.aws." + account + ".accessKey", config.getProperty("edda.aws.accessKey")),
                    config.getProperty("edda.aws." + account + ".secretKey", config.getProperty("edda.aws.secretKey")),
                    config.getProperty("edda." + account + ".region",    config.getProperty("edda.region"))
                )
            }
        ).toMap

        val collMap = if( accounts.size > 1 ) {
            // this give us:
            // Map[String,Array[com.netflix.edda.Collection]]
            val accountCollections = accounts.flatMap(
                account => {
                    mkCollections(accountContexts(account), elector, dsFactory).map(
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
        } else {
            mkCollections(accountContexts(accounts(0)), elector, dsFactory).map( 
                collection => collection.rootName -> collection
            ).toMap
        }

        collMap.foreach( pair => CollectionManager.register(pair._1,pair._2) )
    }
        
    def mkCollections(  ctx: AwsCollection.Context, elector: Elector, dsFactory: String => Option[Datastore]): Seq[AwsCollection] = {
        val res = new AwsReservationCollection(dsFactory, elector, ctx)
        val elb = new AwsLoadBalancerCollection(dsFactory, elector, ctx)
        val asg = new AwsAutoScalingGroupCollection(dsFactory, elector, ctx)
        val inst = new AwsInstanceCollection(res.crawler, dsFactory, elector, ctx)
        return Seq(
            new AwsAddressCollection(dsFactory, elector, ctx),
            asg,
            new AwsImageCollection(dsFactory, elector, ctx),
            elb,
            new AwsInstanceHealthCollection(elb.crawler, dsFactory, elector, ctx),
            new AwsLaunchConfigurationCollection(dsFactory, elector, ctx),
            res,
            inst,
            new AwsSecurityGroupCollection(dsFactory, elector, ctx),
            new AwsSnapshotCollection(dsFactory, elector, ctx),
            new AwsTagCollection(dsFactory, elector, ctx),
            new AwsVolumeCollection(dsFactory, elector, ctx),
            new AwsBucketCollection(dsFactory, elector, ctx),
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
                "platform" -> instanceData.get("platform").getOrElse(null),
                "privateIpAddress" -> instanceData.get("privateIpAddress").getOrElse(null),
                "publicDnsName" -> instanceData.get("publicDnsName").getOrElse(null),
                "publicIpAddress" -> instanceData.get("publicIpAddress").getOrElse(null),
                "start" -> instance.ctime,
                "state" -> asgInst("lifecycleState")
            )
        })
        newInstances
    }
}

abstract class AwsCollection(val rootName: String, ctx: AwsCollection.Context) extends Collection(ctx) {
    val name = ctx.awsClient.accountName match {
        case "" => rootName
        case x: String => x + "." + rootName
    }
}

class AwsAddressCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.addresses", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsAddressCrawler(name, ctx)
}

class AwsAutoScalingGroupCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.autoScalingGroups", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsAutoScalingGroupCrawler(name, ctx)
}

class AwsImageCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.images", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsImageCrawler(name, ctx)
}

class AwsLoadBalancerCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.loadBalancers", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsLoadBalancerCrawler(name, ctx)
}

class AwsInstanceHealthCollection(
    val elbCrawler: AwsLoadBalancerCrawler,
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("view.loadBalancerInstances", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsInstanceHealthCrawler(name, ctx, elbCrawler)
}

class AwsLaunchConfigurationCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.launchConfigurations", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsLaunchConfigurationCrawler(name, ctx)
}

class AwsReservationCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.instances", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsReservationCrawler(name, ctx)
}

class AwsInstanceCollection(
    val resCrawler: AwsReservationCrawler,
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("view.instances", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsInstanceCrawler(name, ctx, resCrawler)
}

class AwsSecurityGroupCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.securityGroups", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsSecurityGroupCrawler(name, ctx)
}

class AwsSnapshotCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.snapshots", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsSnapshotCrawler(name, ctx)
}

class AwsTagCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.tags", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsTagCrawler(name, ctx)
}

class AwsVolumeCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.volumes", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsVolumeCrawler(name, ctx)
}

class AwsBucketCollection(
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx : AwsCollection.Context
) extends AwsCollection("aws.buckets", ctx) {
    val datastore: Option[Datastore] = dsFactory(name)
    val crawler = new AwsBucketCrawler(name, ctx)
}

class GroupAutoScalingGroups(
    val asgCollection: AwsAutoScalingGroupCollection,
    val instanceCollection: AwsInstanceCollection,
    dsFactory: String => Option[Datastore],
    val elector: Elector,
    val ctx: AwsCollection.Context
) extends AwsCollection("group.autoScalingGroups", ctx) with GroupCollection {
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

        val modNewRecords = newRecords.map(
            asgRec => {
                val newInstances = assignSlots(
                    AwsCollection.makeGroupInstances(asgRec, instanceMap),
                    "instanceId",
                    slotMap("instances")
                )
                val asgData = asgRec.data.asInstanceOf[Map[String,Any]]
                val data = Map(
                    "desiredCapacity" -> asgData.get("desiredCapacity").getOrElse(null),
                    "instances" -> newInstances,
                    "launchConfigurationName" -> asgData.get("launchConfigurationName").getOrElse(null),
                    "loadBalancerNames" -> asgData.get("loadBalancerNames").getOrElse(List()),
                    "maxSize" -> asgData.get("maxSize").getOrElse(null),
                    "minSize" -> asgData.get("minSize").getOrElse(null),
                    "name" -> asgRec.id, 
                    "start" -> asgRec.ctime
                )
                
                asgRec.copy(data=data)
            }
        )
        groupDelta(modNewRecords,oldRecords)
    }
}
