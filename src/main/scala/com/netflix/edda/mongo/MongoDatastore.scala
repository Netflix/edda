/*
 * Copyright 2012-2017 Netflix, Inc.
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
import com.netflix.edda.RecordSet
import com.netflix.edda.Collection
import com.netflix.edda.Datastore
import com.netflix.edda.Utils
import com.netflix.edda.RequestId

// http://www.mongodb.org/display/DOCS/Java+Tutorial

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.BasicDBList
import com.mongodb.Mongo
import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.MongoCredential
import com.mongodb.ReadPreference
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
          new DateTime(Option(o.get("ftime")).getOrElse(o.get("ctime")).asInstanceOf[Date]),
          new DateTime(o.get("ctime").asInstanceOf[Date]),
          new DateTime(Option(o.get("stime")).getOrElse(o.get("ctime")).asInstanceOf[Date]),
          Option(o.get("ltime")) match {
            case Some(date: Date) => new DateTime(date)
            case _ => null
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
        o.keySet.asScala.map((key: String) => (mongoDecodeString(key) -> mongoToScala(o.get(key)))).toMap
      }
      case o: BasicDBList => {
        List.empty[Any] ++ o.asScala.map(mongoToScala(_))
      }
      case o: Date => new DateTime(o)
      case o: String => mongoDecodeString(o)
      case o: AnyRef => o
      case null => null
      case other => throw new java.lang.RuntimeException("mongoToScala: don't know how to handle: " + other)
    }
  }

  def mongoDecodeString(str: String): String = {
    str.replace("\\uFF0E", ".").replace("\\uFF04", "$")
  }

  def mongoEncodeString(str: String): String = {
    str.replaceAll("[\\.]", "\\\\uFF0E").replaceAll("[\\$]", "\\\\uFF04")
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
  def mapToMongo(map: Map[String, Any], literal: Boolean = false): DBObject = {
    val obj = new BasicDBObject
    if (literal) {
      map.foreach(pair => obj.put(pair._1, scalaToMongo(pair._2, literal)))
    } else {
      map.foreach(pair => obj.put(mongoEncodeString(pair._1), scalaToMongo(pair._2, literal)))
    }
    obj
  }

  /** converts a Scala basic type to a corresponding Mongo data type */
  def scalaToMongo(obj: Any, literal: Boolean = false): AnyRef = {
    obj match {
      case o: Map[_, _] => mapToMongo(o.asInstanceOf[Map[String, Any]], literal)
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
  def mongoProperty(propName: String, dsName: String, dflt: String): String = {
    Utils.getProperty("edda", "mongo." + propName, "datastore." + dsName, dflt).get
  }

  var primaryMongoConnections: Map[String,Mongo] = Map()
  var replicaMongoConnections: Map[String,Mongo] = Map()

  /** from the collection name string return a Mongo DB Connection */
  def mongoConnection(name: String, replicaOk: Boolean = false): Mongo = {
    import collection.JavaConverters._
    val servers = mongoProperty("address", name, "");
    if( replicaOk && replicaMongoConnections.contains(servers) )
        replicaMongoConnections(servers)
    else if( !replicaOk && primaryMongoConnections.contains(servers) )
        primaryMongoConnections(servers)
    else {
        val serverList = util.Random.shuffle(
            servers.split(',').map(
                hostport => {
                    val parts = hostport.split(':')
                    if (parts.length > 1) {
                        new ServerAddress(parts(0), parts(1).toInt)
                    } else {
                        new ServerAddress(parts(0))
                    }
                }).toList
        )

        val queryTimeout = Utils.getProperty("edda.collection", "queryTimeout", name, "60000").get.toInt
        val user = mongoProperty("user", name, "")

        var credential = List[MongoCredential]()
        if (!user.isEmpty) {
          credential = List(MongoCredential.createMongoCRCredential(
            user,
            mongoProperty("database", name, "edda"),
            mongoProperty("password", name, "").toArray))
        }

        val options = new MongoClientOptions.Builder()
        options.connectTimeout(500)
        options.connectionsPerHost(40)
        options.socketKeepAlive(true)
        options.socketTimeout(queryTimeout)
        options.threadsAllowedToBlockForConnectionMultiplier(8)

        val primary = new MongoClient(serverList.asJava, credential.asJava, options.build())
        primaryMongoConnections += (servers -> primary)

        val replica = new MongoClient(serverList.asJava, credential.asJava, options.build())
        replica.setReadPreference(ReadPreference.secondaryPreferred())
        replicaMongoConnections += (servers -> replica)

        if(replicaOk) replica else primary
    }
  }

  /** from the collection name string return a Mongo Collection (creates the collection
    * if it does not exist) */
  def mongoCollection(name: String, replicaOk: Boolean = false) = {
    val conn = mongoConnection(name, replicaOk)
    val db = conn.getDB(mongoProperty("database", name, "edda"))
    if (db.collectionExists(name)) db.getCollection(name) else db.createCollection(name, null)
  }

}

/** [[com.netflix.edda.Datastore]] subclass that allows MongoDB to be used
 *
 * @param name the name of the collection the datastore is for
 */
class MongoDatastore(val name: String) extends Datastore {

  import MongoDatastore._
  import Collection.RetentionPolicy._

  lazy val primary = mongoCollection(name)
  lazy val replica = mongoCollection(name, replicaOk=true)
  lazy val monitor = mongoCollection(Utils.getProperty("edda", "monitor.collectionName", "mongo", "sys.monitor").get)

  lazy val retentionPolicy = Utils.getProperty("edda.collection", "retentionPolicy", name, "ALL")

  private[this] val logger = LoggerFactory.getLogger(getClass)

  /** query routine to fetch records from mongoDB.
    *
    * @param queryMap query criteria
    * @param limit restrict returned record count, 0 == unlimited
    * @param keys  unless empty Set return only requested keys
    * @param replicaOk reading from a replica in a replSet is OK this is set to true
    * @return the records that match the query
    */
  override def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean)(implicit req: RequestId): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    val mtime = collectionModified
    val mongoKeys = if (keys.isEmpty) null else mapToMongo(keys.map(_ -> 1).toMap)
    val t0 = System.nanoTime()
    val cursor = {
      val mongo = if(replicaOk) replica else primary
      val cur = mongo.find(mapToMongo(queryMap), mongoKeys)
      if( limit > 0 ) cur.sort(stimeIdSort).limit(limit) else cur
    }
    try {
      val seq = cursor.asScala.toStream.map(mongoToRecord(_)).map(r => if(r.ltime == null ) r.copy(mtime=mtime) else r)
      if( limit > 0 ) seq else seq.sortWith((a, b) => a.stime.isAfter(b.stime))
    } catch {
       case e: Exception => {
         if (logger.isErrorEnabled) logger.error(s"$req$this query failed: $queryMap limit: $limit keys: $keys replicaOk: replicaOk", e)
            throw e
        }
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req$this query: $queryMap $lapse: ${lapse}ms")
      cursor.close()
    }
  }

  /** load records from the collection MongoDB table
    *
    * @param replicaOk reading from a replica in a replSet is OK if this is set to true
    * @return the active records (ltime == null) from the collection
    */
  override def load(replicaOk: Boolean)(implicit req: RequestId): RecordSet = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    val mtime = collectionModified
    val cursor = {
      val mongo = if(replicaOk) replica else primary
      mongo.find(nullLtimeQuery)
    }
    try {
      val x = cursor.asScala.map(mongoToRecord(_)).toSeq.map(_.copy(mtime=mtime)).sortWith((a, b) => a.stime.isAfter(b.stime))
      if (logger.isInfoEnabled) logger.info(s"$req$this Loaded ${x.size} records")
      RecordSet(x, Map("mtime" -> collectionModified() ))
    } catch {
      case e: Exception => {
        throw new java.lang.RuntimeException(this + " failed to load", e)
      }
    } finally {
      cursor.close()
    }
  }

  /** update records, delete removed records, insert added records */
  override def update(d: Collection.Delta)(implicit req: RequestId): Collection.Delta = {
    var toRemove: Seq[Record] = Seq();
    val records = d.removed ++ d.added ++ d.changed.flatMap(
      pair => {
        // only update oldRecord if the stime is changed, this allows
        // for inplace updates when we dont want to create new document
        // revision, but still want the record updated
        if (pair.oldRecord.stime == pair.newRecord.stime) {
          Seq(pair.newRecord)
        } else if (Collection.RetentionPolicy.withName(retentionPolicy.get) == LAST) {
          toRemove = pair.oldRecord +: toRemove
          Seq(pair.newRecord)
        } else {
          Seq(pair.oldRecord, pair.newRecord)
        }
      })

    records.foreach( r => if (Collection.RetentionPolicy.withName(retentionPolicy.get) == LIVE && r.ltime != null) remove(r) else upsert(r) )
    toRemove.foreach( remove(_) )
    markCollectionModified
    d
  }

  def collectionModified()(implicit req: RequestId): DateTime  = {
      val rec = monitor.findOne(mapToMongo(Map("_id" -> name)));
      if( rec == null ) DateTime.now() else mongoToRecord(rec).mtime
  }

  def markCollectionModified()(implicit req: RequestId) = {
    try {
      val now = DateTime.now()
      monitor.findAndModify(
        mapToMongo(Map("_id" -> name)),
        null, // fields
        null, // sort
        false, // remove
        mapToMongo( // update
          Map(
          "_id" -> name,
          "id" -> name,
          "ftime" -> now,
          "ctime" -> now,
          "mtime" -> now,
          "stime" -> now,
          "ltime" -> null,
          "data" -> Map("updated" -> now, "id" -> name, "type" -> "collection"))
        ),
        false, // returnNew
        true // upsert
      )
    } catch {
      case e: Exception => {
        if (logger.isErrorEnabled) logger.error(s"$req$this failed to update collection mtime", e)
        throw e
      }
    }
  }

  /** ensures Indes for "stime", "mtime", "ltime", and "id" */
  def init() {
    primary.createIndex(mapToMongo(Map("stime" -> -1, "id" -> 1)))
    primary.createIndex(mapToMongo(Map("stime" -> -1)))
    primary.createIndex(mapToMongo(Map("mtime" -> -1)))
    primary.createIndex(mapToMongo(Map("ltime" -> 1)))
    primary.createIndex(mapToMongo(Map("id" -> 1)))
  }

  protected def upsert(record: Record)(implicit req: RequestId) {
    try {
      primary.findAndModify(
        mapToMongo(Map("_id" -> (record.toId()))), // query
        null, // fields
        null, // sort
        false, // remove
        recordToMongo(record), // update
        false, // returnNew
        true // upsert
      )
    } catch {
      case e: Exception => {
        if (logger.isErrorEnabled) logger.error(s"$req$this failed to upsert record: $record")
        throw e
      }
    }
  }

  protected def remove(record: Record)(implicit req: RequestId) {
    remove(Map("_id" -> (record.toId())));
  }

  override def remove(queryMap: Map[String, Any])(implicit req: RequestId) {
    try {
      var opResult = primary.remove(mapToMongo(queryMap, true))
      logger.info("{}{} removed {} records", Array(req, this, opResult.getN()))
    } catch {
      case e: Exception => {
        if (logger.isErrorEnabled) logger.error(s"$req$this failed to remove records: $queryMap")
        throw e
      }
    }
  }

  override def toString = "[MongoDatastore " + name + "]"
}
