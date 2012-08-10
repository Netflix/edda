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
    val nullLtimeQuery = {
        val obj = new BasicDBObject
        obj.put("ltime", null)
        obj
    }
    val stimeIdSort = {
        val obj = new BasicDBObject
        obj.put("stime", -1)
        obj.put("id", 1)
        obj
    }
}

class MongoCollection(mongo: DBCollection, crawler: Crawler, elector: Elector) extends Collection(crawler, elector) {
    import Collection._
    import MongoCollection._

    override protected
    def doQuery(queryMap: Map[String,Any], state: StateMachine.State): List[Record] = {
        // generate function
        localState(state).records.filter( record => matcher.doesMatch(queryMap, record.toMap ) )
    }

    protected
    def load(state: StateMachine.State): List[Record] = {
        import collection.JavaConversions._ // for Iterator
        val cursor = mongo.find(nullLtimeQuery).sort(stimeIdSort);
        try {
            asScalaIterator(cursor).map( mongoToRecord(_) ).toList
        } finally {
            cursor.close();
        }
    }
    
    protected
    def update(d: Delta, state: StateMachine.State) {
    }

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
            case _ => {
                null
            }
        }
    }
}
