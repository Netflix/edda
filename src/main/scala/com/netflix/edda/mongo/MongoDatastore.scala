package com.netflix.edda.mongo

import com.netflix.edda.Record
import com.netflix.edda.Collection
import com.netflix.edda.Datastore
import com.netflix.edda.ConfigContext

// http://www.mongodb.org/display/DOCS/Java+Tutorial

import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.BasicDBList
import com.mongodb.Mongo
import com.mongodb.ServerAddress

import org.joda.time.DateTime
import java.util.Date

import com.weiglewilczek.slf4s.Logger

object MongoDatastore {
    
    val nullLtimeQuery = mapToMongo( Map("ltime" -> null) )
    val stimeIdSort = mapToMongo( Map("stime" -> -1, "id" -> 1) )

    def mongoToRecord(obj: DBObject): Record = {
        obj match {
            case o: BasicDBObject =>
                Record(
                    Option(o.get("id")).getOrElse(o.get("_id")).asInstanceOf[String],
                    new DateTime(o.get("ctime").asInstanceOf[Date]),
                    new DateTime(Option(o.get("stime")).getOrElse(o.get("ctime")).asInstanceOf[Date]),
                    new DateTime(o.get("ltime").asInstanceOf[Date]),
                    new DateTime(o.get("mtime").asInstanceOf[Date]),
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
            case o: BasicDBList => {
                o.toList.map( mongoToScala(_) )
            }
            case o: Date => new DateTime(o)
            case o: AnyRef => o
            case null => null
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
            case o: DateTime => o.toDate
            case o: AnyRef => o
            case null => null
            case other => throw new java.lang.RuntimeException("scalaToMongo: don't know how to handle: " + other)
        }
    }

    def mongoCollection(name: String,  ctx: ConfigContext) = {
        import collection.JavaConversions._
        val servers = ctx.config.getProperty("edda.mongo.address").split(',').map(
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
        val db = conn.getDB( ctx.config.getProperty("edda.mongo.database", "edda") )
        if( ctx.config.getProperty("edda.mongo.user") != null ) {
            db.authenticate(
                ctx.config.getProperty("edda.mongo.user"),
                ctx.config.getProperty("edda.mongo.password").toArray
            )
        }
        if( db.collectionExists(name) ) db.getCollection(name) else db.createCollection(name, null)
    }

}

class MongoDatastore(ctx: ConfigContext, val name: String) extends Datastore {
    import MongoDatastore._

    val mongo = mongoCollection(name,ctx)

    private[this] val logger = Logger(getClass)

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
            val x = asScalaIterator(cursor).map( mongoToRecord(_) ).toList
            logger.info(this + " Loaded " + x.size + " records")
            x
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
        try {
            mongo.findAndModify(
                mapToMongo(Map("_id" -> (record.id + "|" + record.stime.getMillis))), // query
                null,  // fields
                null,  // sort
                false, // remove
                recordToMongo(record), // update
                    false, // returnNew
                true   // upsert
            )
        } catch {
            case e => logger.error("failed to upsert record: " + record, e)
        }
    }

    override def toString = "[MongoDatastore " + name + "]";
}
