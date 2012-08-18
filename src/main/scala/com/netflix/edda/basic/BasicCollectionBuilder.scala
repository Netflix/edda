package com.netflix.edda.basic

import com.netflix.edda.Collection
import com.netflix.edda.aws.AwsClient
import com.netflix.edda.aws.AwsCrawler
import com.netflix.edda.aws.AwsCrawlerBuilder
import com.netflix.edda.Crawler
import com.netflix.edda.ConfigContext

import com.netflix.edda.mongo.MongoDatastore
import com.netflix.edda.mongo.MongoElector

import java.util.Properties

import com.weiglewilczek.slf4s.Logger

class BasicCollectionBuilder {

    private[this] val logger = Logger(getClass)
    
	val context = new ConfigContext with AwsCrawler.Context with Collection.Context {
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
        
        val awsClient = new AwsClient(
            config.getProperty("edda.aws.accessKey"),
            config.getProperty("edda.aws.secretKey"),
            config.getProperty("edda.aws.region")
        )

        val recordMatcher = new BasicRecordMatcher
        val bm = new BasicBeanMapper(this)
        def beanMapper = bm
   }

    def build(): Map[String,Collection] = {
        AwsCrawlerBuilder.build(context).map( 
            pair => pair._1 -> buildCollection(pair._1, pair._2)
        ).toMap
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
