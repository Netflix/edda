package com.netflix.edda.aws

import com.netflix.edda.Collection
import com.netflix.edda.MergedCollection
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
    def buildAll(ctx: Collection.Context, bm: BeanMapper, elector: Elector, dsFactory: String => Option[Datastore]): Map[String,Queryable] = {
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
                    config.getProperty("edda.aws." + account + ".region",    config.getProperty("edda.aws.region"))
                )
            }
        ).toMap

        if( accounts.size > 1 ) {
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
) extends AwsCollection("group.autoScalingGroups", ctx) {
    val crawler = asgCollection.crawler
    val datastore: Option[Datastore] = dsFactory(name)

    private[this] val logger = LoggerFactory.getLogger(getClass)

    // we dont need to refresh out crawler/collection, it will be 
    // done for us by the AwsAutoScalingGroupCollection
    override protected
    def refresher = Unit

    implicit def recordOrdering: Ordering[Record] = Ordering.fromLessThan(_.stime isBefore _.stime)
    
    override
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, state: StateMachine.State): Seq[Record] = {
        val records = super.doQuery(queryMap,limit,live,state)
        records.groupBy(_.id).values.toSeq.sortBy(_.head).map( mergeRecords(_) )
    }

    def mergeRecords(records: Seq[Record]): Record = {
        if( records.size == 1 ) {
            return records.head
        }

        var seen: Set[String] = Set()
        val instances = records.map(
            rec => {
                rec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[List[Map[String,Any]]].map(
                    inst => inst ++ Map("end" -> rec.ltime)
                )
            }
        ).flatten.filterNot(
            inst => {
                val id = inst("instanceId").asInstanceOf[String]
                val skip = seen.contains(id)
                if( !skip ) {
                    seen = seen + id
                }
                skip
            }
        )
        
        val rec = records.head
        val data = rec.data.asInstanceOf[Map[String,Any]] ++ Map("instances" -> instances, "end" -> rec.ltime)
        return rec.copy(data=data)
    }

    override protected
    def delta(newRecords: Seq[Record], oldRecords: Seq[Record]) =  {
        // newRecords will be from the ASG crawler, we need to convert it
        // to the Group records

        val oldMap = oldRecords.map( rec => rec.id -> rec).toMap

        val instanceSlots = oldRecords.flatMap( rec => {
            rec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[List[Map[String,Any]]].map(
                inst => inst("instanceId").asInstanceOf[String] -> inst("slot").asInstanceOf[Int] )
        }).toMap

        val instanceMap = instanceCollection.query(Map.empty).map(rec => rec.id -> rec).toMap

        val modNewRecords = newRecords.map(
            asgRec => {
                val instances = asgRec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[List[Map[String,Any]]]
                val usedSlots: Set[Int] = instances.map(
                    inst => inst("instanceId").asInstanceOf[String]
                ) collect {
                    case id: String if instanceSlots.contains(id) => instanceSlots(id)
                } toSet
                var unusedSlots = Range(0, instances.size).collect {
                    case slot if !usedSlots.contains(slot) => slot
                }

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
                    val slot = instanceSlots.get(id) match {
                        case Some(slot) => slot
                        case None => {
                            val slot = unusedSlots.head
                            unusedSlots = unusedSlots.tail
                            slot
                        }
                    }
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
                        "slot"  -> slot,
                        "start" -> instance.ctime,
                        "state" -> asgInst("lifecycleState")
                    )
                }).sortWith( (a,b) => a("slot").asInstanceOf[Int] < b("slot").asInstanceOf[Int] )

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

        val newMap = modNewRecords.map( rec => rec.id -> rec).toMap
        
        val now=DateTime.now

        val removed = oldRecords.filterNot(
            rec => newMap.contains(rec.id)
        ).map(
            rec => rec.copy(mtime=now,ltime=now)
        )

        val added = modNewRecords.filterNot( rec => oldMap.contains(rec.id) )
        
        val changes = modNewRecords.filter( rec => {
            oldMap.contains(rec.id) && newMap.contains(rec.id) && !newMap(rec.id).sameData(oldMap(rec.id))
        }).map( rec => {
            // if we have new instances then we increment stime, otherwise just update to new document
            val newInstanceSet = rec.data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[List[Map[String,Any]]].map(
                inst => inst("instanceId").asInstanceOf[String]
            ).toSet

            val oldInstanceSet = oldMap(rec.id).data.asInstanceOf[Map[String,Any]]("instances").asInstanceOf[List[Map[String,Any]]].map(
                inst => inst("instanceId").asInstanceOf[String]
            ).toSet
            
            val oldRec = oldMap(rec.id)
            if( newInstanceSet == oldInstanceSet ) {
                rec.id -> Collection.RecordUpdate(oldRec, rec.copy(stime=oldRec.stime))
            } else {
                // sets dont have same instances, so create new document revision
                rec.id -> Collection.RecordUpdate(oldRec.copy(mtime=now,ltime=now), rec)
            }
        }).toMap

        // need to reset stime,ctime,tags for crawled records to match what we have in memory
        val fixedRecords = modNewRecords.collect {
            case rec: Record if changes.contains(rec.id) => {
                val newRec = changes(rec.id).newRecord
                oldMap(rec.id).copy(data=rec.data, mtime=newRec.mtime, stime=newRec.stime)
            }
            case rec: Record if oldMap.contains(rec.id) => 
                oldMap(rec.id).copy(data=rec.data, mtime=rec.mtime)
            case rec: Record => rec
        }

        Collection.Delta(fixedRecords, changed=changes.values.toSeq, added=added, removed=removed)
    }
}
