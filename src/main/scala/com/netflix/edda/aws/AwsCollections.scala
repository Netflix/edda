package com.netflix.edda.aws

import com.netflix.edda.Collection
import com.netflix.edda.MergedCollection
import com.netflix.edda.Crawler
import com.netflix.edda.Elector
import com.netflix.edda.Queryable

import com.netflix.edda.Datastore
import com.netflix.edda.BeanMapper

object AwsCollectionBuilder {
    def buildAll(ctx: Collection.Context, bm: BeanMapper, elector: Elector, dsFactory: String => Option[Datastore]): Map[String,Queryable] = {
        val accounts = ctx.config.getProperty("edda.aws.accounts","").split(",");
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
        return Seq(
            new AwsAddressCollection(dsFactory, elector, ctx),
            new AwsAutoScalingGroupCollection(dsFactory, elector, ctx),
            new AwsImageCollection(dsFactory, elector, ctx),
            elb,
            new AwsInstanceHealthCollection(elb.crawler, dsFactory, elector, ctx),
            new AwsLaunchConfigurationCollection(dsFactory, elector, ctx),
            res,
            new AwsInstanceCollection(res.crawler, dsFactory, elector, ctx),
            new AwsSecurityGroupCollection(dsFactory, elector, ctx),
            new AwsSnapshotCollection(dsFactory, elector, ctx),
            new AwsTagCollection(dsFactory, elector, ctx),
            new AwsVolumeCollection(dsFactory, elector, ctx),
            new AwsBucketCollection(dsFactory, elector, ctx)
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
