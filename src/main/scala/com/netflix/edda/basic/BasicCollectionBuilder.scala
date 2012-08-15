package com.netflix.edda.basic

import com.netflix.edda.Collection
import com.netflix.edda.aws.AwsClient
import com.netflix.edda.aws.AwsCrawlerBuilder
import com.netflix.edda.Crawler
// import com.netflix.edda.Datastore
// import com.netflix.edda.Elector

import com.netflix.edda.mongo.MongoDatastore
import com.netflix.edda.mongo.MongoElector

import com.netflix.edda.RecordMatcherComponent
import com.netflix.edda.ConfigurationComponent
import com.netflix.edda.aws.AwsClientComponent
import com.netflix.edda.ElectorComponent
import com.netflix.edda.NamedComponent

import java.util.Properties

import org.slf4j.{Logger,LoggerFactory}

class BasicCollectionBuilder {
    trait CommonComponents extends ConfigurationComponent with ElectorComponent with AwsClientComponent with RecordMatcherComponent {
        private[this] val logger = LoggerFactory.getLogger(getClass)
        val propFile = System.getProperty("edda.properties", "/edda.properties");
        val props = new Properties();
        try {
            val inputStream = classOf[Server].getResourceAsStream(propFile);
            try {
                props.load(inputStream);
            } finally {
                inputStream.close();
            }
        } catch {
            case e => logger.error("Unable to load properties file " + propFile
                                   + " set System property \"edda.properties\" to valid file", e);
        }
        
        val awsClient = new AwsClient(
            props.getProperty("edda.aws.accessKey"),
            props.getProperty("edda.aws.secretKey"),
            props.getProperty("edda.aws.region")
        )
        val elector   = new MongoElector with CommonComponents
        val config    = props
        val recordMatcher = new BasicRecordMatcher
    }
    
    def build(): Map[String,Collection] = {
        (new AwsCrawlerBuilder with CommonComponents).build().map( 
            pair => pair._1 -> buildCollection(pair._1, pair._2)
        ).toMap
    }

    def buildCollection(localName: String, localCrawler: Crawler): Collection = {
        trait CommonName extends NamedComponent { val name = localName }
        new Collection with CommonComponents with CommonName {
            val datastore = Some(new MongoDatastore with CommonComponents with CommonName)
            val crawler = localCrawler
        }
    }
}
