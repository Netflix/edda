package com.netflix.edda.basic

import com.netflix.edda.Collection
import com.netflix.edda.Queryable
import com.netflix.edda.MergedCollection
import com.netflix.edda.aws.AwsClient
import com.netflix.edda.aws.AwsCrawler
import com.netflix.edda.aws.AwsCrawlerBuilder
import com.netflix.edda.Crawler
import com.netflix.edda.ConfigContext

import com.netflix.edda.mongo.MongoDatastore
import com.netflix.edda.mongo.MongoElector

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

class BasicCollectionBuilder {

    private[this] val logger = LoggerFactory.getLogger(getClass)
    
	val context = new ConfigContext with Collection.Context {
        val propFile = System.getProperty("edda.properties", "/edda.properties");
        val props = new Properties();
        try {
            val inputStream = getClass.getResourceAsStream(propFile);
            try {
                props.load(inputStream);
            } finally {
                inputStream.close();
            }
        } catch {
            case e => logger.error("Unable to load properties file " + propFile
                                   + " set System property \"edda.properties\" to valid file", e);
        }
        val config    = props
        
        val recordMatcher = new BasicRecordMatcher
        val bm = new BasicBeanMapper(this)
        def beanMapper = bm
   }

    def build(): Map[String,Queryable] = {
        val accounts = context.config.getProperty("edda.aws.accounts","").split(",");
        val accountContexts = accounts.map(
            account => account -> new ConfigContext with AwsCrawler.Context {
                val config = context.config
                val beanMapper = context.beanMapper
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
                    AwsCrawlerBuilder.build(accountContexts(account)).map(
                        crawler => {
                            val name = crawler.name
                            crawler.rootName -> buildCollection(name, crawler)
                        }
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
            AwsCrawlerBuilder.build(accountContexts(accounts(0))).map( 
                crawler => crawler.name -> buildCollection(crawler.name, crawler)
            ).toMap
        }
    }

    val localElector = new MongoElector(context)

    def buildCollection(localName: String, localCrawler: Crawler): Collection = {
        val ds = new MongoDatastore(context, localName)
        val coll = new Collection(context) {
            override def name = localName
	        override def crawler = localCrawler
            override def datastore = Some(ds)
            override def elector = localElector
        }
        coll
    }
}
