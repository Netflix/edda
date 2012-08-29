package com.netflix.edda.basic

import com.netflix.edda.Collection
// import com.netflix.edda.Queryable
// import com.netflix.edda.MergedCollection
// import com.netflix.edda.aws.AwsClient
// import com.netflix.edda.aws.AwsCrawler
// import com.netflix.edda.aws.AwsCrawlerBuilder
// import com.netflix.edda.Crawler
// import com.netflix.edda.ConfigContext

// import com.netflix.edda.mongo.MongoDatastore
// import com.netflix.edda.mongo.MongoElector

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

object BasicContext extends Collection.Context {
    private[this] val logger = LoggerFactory.getLogger(getClass)
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
}

    // def build(): Map[String,Queryable] = {
    // }

    // val localElector = new MongoElector(context)

    // def buildCollection(localName: String, localCrawler: Crawler): Collection = {
    //     val ds = new MongoDatastore(context, localName)
    //     val coll = new Collection(context) {
    //         override def name = localName
	//         override def crawler = localCrawler

    //         override def datastore = Some(ds)
    //         override def elector = localElector
    //     }
    //     coll
    // }
// }
