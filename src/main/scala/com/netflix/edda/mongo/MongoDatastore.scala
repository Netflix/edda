package com.netflix.edda.mongo

import com.netflix.edda.Record
import com.netflix.edda.Collection
import com.netflix.edda.Datastore

import com.netflix.edda.NamedComponent
import com.netflix.edda.ConfigurationComponent

// http://www.mongodb.org/display/DOCS/Java+Tutorial

import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.BasicDBList
import com.mongodb.Mongo
import com.mongodb.ServerAddress

import org.joda.time.DateTime

import org.slf4j.{Logger, LoggerFactory}

object MongoDatastore {
    val nullLtimeQuery = mapToMongo( Map("ltime" -> null) )
    val stimeIdSort = mapToMongo( Map("stime" -> -1, "id" -> 1) )
    
    def mongoToRecord(obj: DBObject): Record = {
        obj match {
            case o: BasicDBObject =>
                Record(
                    Option(o.get("id")).getOrElse(o.get("_id")).asInstanceOf[String],
                    o.get("ctime").asInstanceOf[DateTime],
                    Option(o.get("stime")).getOrElse(o.get("ctime")).asInstanceOf[DateTime],
                    o.get("ltime").asInstanceOf[DateTime],
                    o.get("mtime").asInstanceOf[DateTime],
                    mongoToScala(o.get("data")),
                    mongoToScala(o.get("tags")).asInstanceOf[Map[String,String]]
                )
            case other => throw new java.lang.RuntimeException("cannot turn " + other + " into a Record")
        }
    }

    def mongoToScala(obj: Any): Any = {
        import collection.JavaConversions._
        obj match {
            case o: BasicDBObject => {
                o.keySet.toSet.map( (key: String) => (key -> mongoToScala(o.get(key))) ).toMap
            }
            case o: BasicDBList => Unit
            case o: AnyRef => o
            case other => throw new java.lang.RuntimeException("mongoToScala: don't know how to handle: " + other)
        }
    }

    def recordToMongo(rec: Record, id: Option[String] = None): DBObject = {
        val obj = mapToMongo(rec.toMap)
        if( id.isDefined ) {
            obj.put("_id", id.get)
        } else {
            obj.put("_id", rec.id + "|" + rec.stime.getMillis)
        }
        obj
    }

    def mapToMongo(map: Map[String,Any]): DBObject = {
        val obj = new BasicDBObject
        map.foreach( pair => obj.put(pair._1, scalaToMongo(pair._2)) )
        obj
    }

    def scalaToMongo(obj: Any): AnyRef = {
        obj match {
            case o: Map[_,_] => mapToMongo(o.asInstanceOf[Map[String,Any]])
            case o: List[_] => {
                val mongo = new BasicDBList
                o.foreach( item => mongo.add( scalaToMongo( item ) ) )
                mongo
            }
            case o: AnyRef => o
            case other => throw new java.lang.RuntimeException("scalaToMongo: don't know how to handle: " + other)
        }
    }
}

trait MongoDatastore extends Datastore with NamedComponent with ConfigurationComponent {
    import MongoDatastore._

    private[this] val logger = LoggerFactory.getLogger(getClass)

    val mongo = {
        import collection.JavaConversions._
        val servers = config.getProperty("edda.mongo.address").split(',').map(
            hostport => {
                val parts = hostport.split(':')
                if( parts.length > 1 ) {
                    new ServerAddress( parts(0), parts(1).toInt )
                } else {
                    new ServerAddress( parts(0) )
                }
            }
        )
        val conn = new Mongo( servers.toList )
        val db = conn.getDB( config.getProperty("edda.mongo.database", "edda") )
        if( config.getProperty("edda.mongo.user") != null ) {
            db.authenticate(
                config.getProperty("edda.mongo.user"),
                config.getProperty("edda.mongo.password").toArray
            )
        }
        db.getCollection(name)
    }

    override
    def query(queryMap: Map[String,Any], limit: Int): List[Record] = {
        import collection.JavaConversions._
        val cursor = mongo.find(mapToMongo(queryMap)).sort(stimeIdSort);
        try {
            asScalaIterator(cursor).map( mongoToRecord(_) ).toList
        } finally {
            cursor.close();
        }
    }

    override
    def load(): List[Record] = {
        import collection.JavaConversions._
        val cursor = mongo.find(nullLtimeQuery).sort(stimeIdSort);
        try {
            asScalaIterator(cursor).map( mongoToRecord(_) ).toList
        } finally {
            cursor.close();
        }
    }
    
    override
    def update(d: Collection.Delta) {
        d.changed.foreach(
            pair => {
                upsert(pair.oldRecord)
                upsert(pair.newRecord)
            }
        )

        d.added.foreach( upsert(_) )
        d.removed.foreach( upsert(_) )
    }

    def init() = mongo.ensureIndex(stimeIdSort)

    protected
    def upsert( record: Record ) {
        mongo.findAndModify(
            mapToMongo(Map("_id" -> (record.id + "|" + record.stime.getMillis))), // query
            null,  // fields
            null,  // sort
            false, // remove
            recordToMongo(record), // update
            false, // returnNew
            true   // upsert
        )
    }

}
