/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda.mongo

import com.netflix.edda.Record
import com.netflix.edda.Collection
import com.netflix.edda.DataStore
import com.netflix.edda.ConfigContext
import com.netflix.edda.Utils

// http://www.mongodb.org/display/DOCS/Java+Tutorial

import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.BasicDBList
import com.mongodb.Mongo
import com.mongodb.ServerAddress
import com.mongodb.Bytes

import org.joda.time.DateTime
import java.util.Date
import java.util.Properties

import org.slf4j.{ Logger, LoggerFactory }

object MongoDatastore {

  val nullLtimeQuery = mapToMongo(Map("ltime" -> null))
  val stimeIdSort = mapToMongo(Map("stime" -> -1, "id" -> 1))

  def mongoToRecord(obj: DBObject): Record = {
    obj match {
      case o: BasicDBObject =>
        Record(
          Option(o.get("id")).getOrElse(o.get("_id")).asInstanceOf[String],
          new DateTime(o.get("ctime").asInstanceOf[Date]),
          new DateTime(Option(o.get("stime")).getOrElse(o.get("ctime")).asInstanceOf[Date]),
          Option(o.get("ltime")) match { case Some(date: Date) => new DateTime(date); case None => null },
          new DateTime(o.get("mtime").asInstanceOf[Date]),
          mongoToScala(o.get("data")),
          mongoToScala(o.get("tags")).asInstanceOf[Map[String, Any]])
      case other => throw new java.lang.RuntimeException("cannot turn " + other + " into a Record")
    }
  }

  def mongoToScala(obj: Any): Any = {
    import collection.JavaConverters._
    obj match {
      case o: BasicDBObject => {
        o.keySet.asScala.map((key: String) => (key -> mongoToScala(o.get(key)))).toMap
      }
      case o: BasicDBList => {
        List.empty[Any] ++ o.asScala.map(mongoToScala(_))
      }
      case o: Date => new DateTime(o)
      case o: AnyRef => o
      case null => null
      case other => throw new java.lang.RuntimeException("mongoToScala: don't know how to handle: " + other)
    }
  }

  def recordToMongo(rec: Record, id: Option[String] = None): DBObject = {
    val obj = mapToMongo(rec.toMap)
    if (id.isDefined) {
      obj.put("_id", id.get)
    } else {
      obj.put("_id", rec.id + "|" + rec.stime.getMillis)
    }
    obj
  }

  def mapToMongo(map: Map[String, Any]): DBObject = {
    val obj = new BasicDBObject
    map.foreach(pair => obj.put(pair._1, scalaToMongo(pair._2)))
    obj
  }

  def scalaToMongo(obj: Any): AnyRef = {
    obj match {
      case o: Map[_, _] => mapToMongo(o.asInstanceOf[Map[String, Any]])
      case o: Seq[_] => {
        val mongo = new BasicDBList
        o.foreach(item => mongo.add(scalaToMongo(item)))
        mongo
      }
      case o: DateTime => o.toDate
      case o: AnyRef => o
      case null => null
      case other => throw new java.lang.RuntimeException("scalaToMongo: don't know how to handle: " + other)
    }
  }

  def mongoProperty(props: Properties, propName: String, dsName: String, dflt: String): String = {
    Utils.getProperty(props, "edda", "mongo." + propName, "datastore." + dsName, dflt)
  }

  def mongoConnection(name: String, ctx: ConfigContext) = {
    import collection.JavaConverters._
    val servers = mongoProperty(ctx.config, "address", name, "").split(',').map(
      hostport => {
        val parts = hostport.split(':')
        if (parts.length > 1) {
          new ServerAddress(parts(0), parts(1).toInt)
        } else {
          new ServerAddress(parts(0))
        }
      }).toList
    new Mongo(servers.asJava)
  }

  def mongoCollection(name: String, ctx: ConfigContext) = {
    val conn = mongoConnection(name, ctx)
    val db = conn.getDB(mongoProperty(ctx.config, "database", name, "edda"))
    val user = mongoProperty(ctx.config, "user", name, null)
    if (user != null) {
      db.authenticate(
        user,
        mongoProperty(ctx.config, "password", name, "").toArray)
    }
    if (db.collectionExists(name)) db.getCollection(name) else db.createCollection(name, null)
  }

}

class MongoDatastore(ctx: ConfigContext, val name: String) extends DataStore {
  import MongoDatastore._

  val mongo = mongoCollection(name, ctx)

  private[this] val logger = LoggerFactory.getLogger(getClass)

  override def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    logger.info(this + " query: " + queryMap)
    val mongoKeys = if (keys.isEmpty) null else mapToMongo(keys.map(_ -> 1).toMap)
    val cursor = {
        val cur = mongo.find(mapToMongo(queryMap), mongoKeys)
        if( replicaOk ) cur.addOption(Bytes.QUERYOPTION_SLAVEOK)
        cur.sort(stimeIdSort)
    }
    try {
      cursor.asScala.toStream.map(mongoToRecord(_))
    } finally {
      cursor.close()
    }
  }

  override def load(replicaOk: Boolean): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    val cursor = {
        val cur = mongo.find(nullLtimeQuery)
        if( replicaOk ) cur.addOption(Bytes.QUERYOPTION_SLAVEOK)
        cur.sort(stimeIdSort)
    }
    try {
      val x = cursor.asScala.map(mongoToRecord(_)).toSeq
      logger.info(this + " Loaded " + x.size + " records")
      x
    } finally {
      cursor.close()
    }
  }

  override def update(d: Collection.Delta) {
    d.changed.foreach(
      pair => {
        // logger.info("UPDATE\nOld: " + pair.oldRecord + "\nNew: " + pair.newRecord)
        // only update oldRecord if the stime is changed
        if (pair.oldRecord.stime != pair.newRecord.stime) {
          upsert(pair.oldRecord)
        }
        upsert(pair.newRecord)
      })

    d.added.foreach(upsert(_))
    d.removed.foreach(upsert(_))
  }

  def init() = {
    mongo.ensureIndex(mapToMongo(Map("stime" -> -1)))
    mongo.ensureIndex(mapToMongo(Map("ltime" -> 1)))
    mongo.ensureIndex(mapToMongo(Map("id" -> 1)))
  }

  protected def upsert(record: Record) {
    try {
      mongo.findAndModify(
        mapToMongo(Map("_id" -> (record.id + "|" + record.stime.getMillis))), // query
        null, // fields
        null, // sort
        false, // remove
        recordToMongo(record), // update
        false, // returnNew
        true // upsert
        )
    } catch {
      case e => {
        logger.error("failed to upsert record: " + record)
        throw e
      }
    }
  }

  override def toString = "[MongoDatastore " + name + "]";
}
