/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.BasicDBList
import com.mongodb.Mongo
import com.mongodb.ServerAddress
import com.mongodb.Bytes

import org.joda.time.DateTime
import java.util.Date
import java.util.Properties

import org.slf4j.LoggerFactory

/** helper object to store common Mongo related routines */
object MongoDatastore {

  val nullLtimeQuery = mapToMongo(Map("ltime" -> null))
  val stimeIdSort = mapToMongo(Map("stime" -> -1, "id" -> 1))

  /** converts a mongo DBObject to a Record */
  def mongoToRecord(obj: DBObject): Record = {
    obj match {
      case o: BasicDBObject =>
        Record(
          Option(o.get("id")).getOrElse(o.get("_id")).asInstanceOf[String],
          new DateTime(o.get("ctime").asInstanceOf[Date]),
          new DateTime(Option(o.get("stime")).getOrElse(o.get("ctime")).asInstanceOf[Date]),
          Option(o.get("ltime")) match {
            case Some(date: Date) => new DateTime(date)
            case None => null
          },
          new DateTime(o.get("mtime").asInstanceOf[Date]),
          mongoToScala(o.get("data")),
          mongoToScala(o.get("tags")).asInstanceOf[Map[String, Any]])
      case other => throw new java.lang.RuntimeException("cannot turn " + other + " into a Record")
    }
  }

  /** converts a mongo object to a corresponding Scala basic object */
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

  /** converts a Record to a Mongo DBObject */
  def recordToMongo(rec: Record, id: Option[String] = None): DBObject = {
    val obj = mapToMongo(rec.toMap)
    if (id.isDefined) {
      obj.put("_id", id.get)
    } else {
      obj.put("_id", rec.id + "|" + rec.stime.getMillis)
    }
    obj
  }

  /** converts a basic scala Map to a Mongo DBObject */
  def mapToMongo(map: Map[String, Any]): DBObject = {
    val obj = new BasicDBObject
    map.foreach(pair => obj.put(pair._1, scalaToMongo(pair._2)))
    obj
  }

  /** converts a Scala basic type to a corresponding Mongo data type */
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

  /** helper to fetch properties for mongo related settings */
  def mongoProperty(props: Properties, propName: String, dsName: String, dflt: String): String = {
    Utils.getProperty(props, "edda", "mongo." + propName, "datastore." + dsName, dflt)
  }

  /** from the collection name string return a Mongo DB Connection */
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

  /** from the collection name string return a Mongo Collection (creates the collection
    * if it does not exist) */
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

/** [[com.netflix.edda.DataStore]] subclass that allows MongoDB to be used
 *
 * @param ctx configuration context to setup how we connect to Mongo
 * @param name the name of the collection the datastore is for
 */
class MongoDatastore(ctx: ConfigContext, val name: String) extends DataStore {

  import MongoDatastore._

  val mongo = mongoCollection(name, ctx)

  private[this] val logger = LoggerFactory.getLogger(getClass)

  /** query routine to fetch records from mongoDB.
    *
    * @param queryMap query criteria
    * @param limit restrict returned record count, 0 == unlimited
    * @param keys  unless empty Set return only requested keys
    * @param replicaOk reading from a replica in a replSet is OK this is set to true
    * @return the records that match the query
    */
  override def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    logger.info(this + " query: " + queryMap)
    val mongoKeys = if (keys.isEmpty) null else mapToMongo(keys.map(_ -> 1).toMap)
    val cursor = {
      val cur = mongo.find(mapToMongo(queryMap), mongoKeys)
      if (replicaOk) cur.addOption(Bytes.QUERYOPTION_SLAVEOK)
      cur.sort(stimeIdSort)
    }
    try {
      cursor.asScala.toStream.map(mongoToRecord(_))
    } finally {
      cursor.close()
    }
  }

  /** load records from the collection MongoDB table
    *
    * @param replicaOk reading from a replica in a replSet is OK if this is set to true
    * @return the active records (ltime == null) from the collection
    */
  override def load(replicaOk: Boolean): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    val cursor = {
      val cur = mongo.find(nullLtimeQuery)
      if (replicaOk) cur.addOption(Bytes.QUERYOPTION_SLAVEOK)
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

  /** update records, delete removed records, insert added records */
  override def update(d: Collection.Delta) {
    d.changed.foreach(
      pair => {
        // only update oldRecord if the stime is changed, this allows
        // for inplace updates when we dont want to create new document
        // revision, but still want the record updated
        if (pair.oldRecord.stime != pair.newRecord.stime) {
          upsert(pair.oldRecord)
        }
        upsert(pair.newRecord)
      })

    d.added.foreach(upsert(_))
    d.removed.foreach(upsert(_))
  }

  /** ensures Indes for "stime", "ltime", and "id" */
  def init() {
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
      case e: Exception => {
        logger.error("failed to upsert record: " + record)
        throw e
      }
    }
  }

  override def toString = "[MongoDatastore " + name + "]"
}
