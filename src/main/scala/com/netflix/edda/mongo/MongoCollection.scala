package com.netflix.edda.mongo

import com.netflix.edda.Crawler
import com.netflix.edda.Elector
import com.netflix.edda.Record
import com.netflix.edda.StateMachine
import com.netflix.edda.Collection

// http://www.mongodb.org/display/DOCS/Java+Tutorial

import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.BasicDBList

import org.joda.time.DateTime

import org.slf4j.{Logger, LoggerFactory}

object MongoCollection {
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

class MongoCollection(mongo: DBCollection, crawler: Crawler, elector: Elector) extends Collection(crawler, elector) {
    import Collection._
    import MongoCollection._

    override protected
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, state: StateMachine.State): List[Record] = {
        if( !live ) {
            super.doQuery(queryMap,limit,live,state)
        }
        else {
            import collection.JavaConversions._
            val cursor = mongo.find(mapToMongo(queryMap)).sort(stimeIdSort);
            try {
                asScalaIterator(cursor).map( mongoToRecord(_) ).toList
            } finally {
                cursor.close();
            }
        }
    }

    protected
    def load(state: StateMachine.State): List[Record] = {
        import collection.JavaConversions._
        val cursor = mongo.find(nullLtimeQuery).sort(stimeIdSort);
        try {
            asScalaIterator(cursor).map( mongoToRecord(_) ).toList
        } finally {
            cursor.close();
        }
    }
    
    protected
    def update(d: Delta, state: StateMachine.State) {
        // case class Delta(records: List[Record], changed: List[RecordUpdate], added: List[Record], removed: List[Record])
        
        d.changed.foreach(
            pair => {
                upsert(pair.oldRecord)
                upsert(pair.newRecord)
            }
        )

        d.added.foreach( upsert(_) )
        d.removed.foreach( upsert(_) )
    }

    protected
    def upsert( record: Record ) {
        mongo.findAndModify(
            mapToMongo(Map("_id" -> (record.id + "|" + record.stime.getMillis))), // query
            null,  // fields
            null,  // sort
            false, // remove
            mapToMongo(record.toMap), // update
            false, // returnNew
            true   // upsert
        )
    }

    protected override
    def init = mongo.ensureIndex(stimeIdSort)
}
