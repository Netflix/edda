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
package com.netflix.edda.elasticsearch

import com.netflix.edda.Record
import com.netflix.edda.Collection
import com.netflix.edda.DataStore
import com.netflix.edda.Utils

// // http://www.mongodb.org/display/DOCS/Java+Tutorial

// import com.mongodb.BasicDBObject
// import com.mongodb.DBObject
// import com.mongodb.BasicDBList
// import com.mongodb.Mongo
// import com.mongodb.MongoOptions
// import com.mongodb.ServerAddress
// import com.mongodb.Bytes

import org.joda.time.DateTime
import java.util.Date
// import java.util.Properties

import org.slf4j.LoggerFactory

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.SearchHitField


// /** helper object to store common Mongo related routines */
object ElasticSearchDatastore {

  import org.joda.time.format.ISODateTimeFormat
  val basicDateTime = ISODateTimeFormat.dateTime

  /** converts a mongo DBObject to a Record */
  def esToRecord(obj: Any): Record = {
    obj match {
      case o: java.util.Map[_,_] =>
        Record(
          Option(o.get("id")).getOrElse(o.get("_id")).asInstanceOf[String],
          Option(o.get("ctime")) match {
            case Some(date:String) => basicDateTime.parseDateTime(date)
            case None => null
          },
          Option(o.get("stime")) match {
            case Some(date:String) => basicDateTime.parseDateTime(date)
            case None => null
          },
          Option(o.get("ltime")) match {
            case Some(date: String) => basicDateTime.parseDateTime(date)
            case None => null
          },
          Option(o.get("mtime")) match {
            case Some(date: String) => basicDateTime.parseDateTime(date)
            case None => null
          },
          Option(o.get("data")) match {
            case Some(data) => esToScala(data)
            case None => null
          },
          Option(o.get("tags")) match {
            case Some(tags) => esToScala(tags).asInstanceOf[Map[String,Any]]
            case None => Map[String,Any]()
          }
        )
      case other => throw new java.lang.RuntimeException("cannot turn " + other + " into a Record")
    }
  }

  // map "data.foo" key to data -> Map(foo -> ....)
  def esFieldsFixup(obj: java.util.Map[String,SearchHitField]): java.util.Map[String,AnyRef] = {
    val newObj = new java.util.HashMap[String,AnyRef]()
    import collection.JavaConverters._
    obj.asScala.foreach(kv => {
      if( kv._1.contains('.') ) {
        // should only be 2 parts, parts.head and parts.tail.head
        // we force only 2 parts when calling addFields when searching, more than
        // 2 fields we cannot tell what data type the field should be
        val parts = kv._1.split('.')
        if( !newObj.containsKey(parts.head) ) {
          newObj.put(parts.head, new java.util.HashMap[String,AnyRef])
        }
        newObj.get(parts.head).asInstanceOf[java.util.Map[String,AnyRef]].put(parts.tail.head, kv._2.getValue)
      } else newObj.put(kv._1, kv._2.getValue)
    })
    newObj
  }

  private val dateTimeRx = """^\d\d\d\d\d\d\d\dT\d\d\d\d\d\d([.]\d\d?\d?)?Z$""".r

  /** converts a mongo object to a corresponding Scala basic object */
  def esToScala(obj: Any): Any = {
    import collection.JavaConverters._
    obj match {
      case o: java.util.Map[_,_] => {
        o.keySet.asScala.map(key => (key.asInstanceOf[String] -> esToScala(o.get(key)))).toMap
      }
      case o: java.util.Collection[_] => {
        List.empty[Any] ++ o.asScala.map(esToScala(_))
      }
      case dateTimeRx() => basicDateTime.parseDateTime(obj.asInstanceOf[String])
      case o: Date => new DateTime(o)
      case o: AnyRef => o
      case null => null
      case other => throw new java.lang.RuntimeException("mongoToScala: don't know how to handle: " + other)
    }
  }

  /** dispatch the match operator to the correct matching routine. */
  protected def esFilterOp(key: String, value: Any, op: String): FilterBuilder = {
    // $eq $ne $gt $lt $gte $lte $exists $in $nin $regex
    op match {
      case "$eq" => Option(value) match {
        case None => FilterBuilders.missingFilter(key).nullValue(true).existence(true)
        case Some(value) => FilterBuilders.termFilter(key, value)
      }
      case "$ne" => FilterBuilders.notFilter(esFilterOp(key, value, "$eq"))
      case "$gt" => value match {
        case v: String => FilterBuilders.rangeFilter(key).from(v).includeLower(false)
        case _ => FilterBuilders.numericRangeFilter(key).from(value).includeLower(false)
      }
      case "$gte" => value match {
        case v: String => FilterBuilders.rangeFilter(key).from(v).includeLower(true)
        case _ => FilterBuilders.numericRangeFilter(key).from(value).includeLower(true)
      }
      case "$lt" => value match {
        case v: String => FilterBuilders.rangeFilter(key).to(v).includeUpper(false)
        case _ => FilterBuilders.numericRangeFilter(key).to(value).includeUpper(false)
      }
      case "$lte" => value match {
        case v: String => FilterBuilders.rangeFilter(key).to(v).includeUpper(true)
        case _ => FilterBuilders.numericRangeFilter(key).to(value).includeUpper(true)
      }
      case "$exists" => FilterBuilders.missingFilter(key).existence(true)
      case "$in" => value.asInstanceOf[Seq[Any]].head match {
        case _: String => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[String]]:_*)
        case _: Long => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Long]]:_*)
        case _: Int => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Int]]:_*)
        case _: Double => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Double]]:_*)
        case _: Float => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Float]]:_*)
        case _: AnyRef => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[AnyRef]]:_*)
      }
      case "$nin" => FilterBuilders.notFilter(esFilterOp(key, value, "$in"))
      case "$regex" => throw new java.lang.UnsupportedOperationException("$regex query not supported")
      case unk => throw new java.lang.RuntimeException("uknown match operation: " + unk)
    }
  }

  def esFilter(queryMap: Map[String, Any]): FilterBuilder = {
    val filters = queryMap.map {
      // { key: { $op1: val, $op2: val } } ==>
      case (key: String, value: Map[_, _]) => {
        if( value.size > 1 ) {
          FilterBuilders.andFilter( value.asInstanceOf[Map[String,Any]].map( kv => esFilter( Map(key -> (kv._1, kv._2)) )).toSeq:_* )
        } else {
          val kv = value.asInstanceOf[Map[String,Any]].head
          esFilter( Map(key -> (kv._1, kv._2)) )
        }
      }
      // { $or: [ {key: value}, {key: value} ] }
      case ("$or", value: Seq[_]) => {
        val filters = value.asInstanceOf[Seq[Map[String,Any]]].map(esFilter(_))
        FilterBuilders.orFilter(filters:_*)
      }
      // { $and: [ {key: value}, {key: value} ] }
      case ("$and", value: Seq[_]) => {
        val filters = value.asInstanceOf[Seq[Map[String,Any]]].map(esFilter(_))
        FilterBuilders.andFilter(filters:_*)
      }
      // { key1: { $op1: val }, key2: { $op2: val } }
      case (key: String, (op: String, value: Any)) => esFilterOp(key, value, op)
      case (key: String, value: Any) => esFilterOp(key,value,"$eq")
      case (key: String, null) => esFilterOp(key,null,"$eq")
    } toSeq
    
    if( queryMap.size > 1 ) {
      FilterBuilders.andFilter(filters.toSeq:_*)
    } else filters.head
  }

  def esQuery(queryMap: Map[String, Any]): QueryBuilder = {
    if( queryMap.isEmpty ) QueryBuilders.matchAllQuery else QueryBuilders.constantScoreQuery(esFilter(queryMap))
  }
}

/** [[com.netflix.edda.DataStore]] subclass that allows MongoDB to be used
 *
 * @param name the name of the collection the datastore is for
 */
class ElasticSearchDatastore(val name: String) extends DataStore {

  import Collection.RetentionPolicy._
  import ElasticSearchDatastore._

  import org.elasticsearch.common.settings.ImmutableSettings
  import org.elasticsearch.common.settings.Settings
  import org.elasticsearch.client.transport.TransportClient
  import org.elasticsearch.client.Client
  import org.elasticsearch.common.transport.InetSocketTransportAddress
  import org.elasticsearch.action.search.SearchOperationThreading._

  lazy val settings: Settings = ImmutableSettings.settingsBuilder().put("cluster.name", Utils.getProperty("edda", "elasticsearch.cluster", name, "edda").get).build()
  lazy val client: Client = Utils.getProperty("edda", "elasticsearch.address", name, "edda").get.split(',').fold(new TransportClient(settings))(
    (client, addr) => {
      val parts = addr.asInstanceOf[String].split(':')
      client.asInstanceOf[TransportClient].addTransportAddress(new InetSocketTransportAddress(parts.head, parts.tail.head.toInt))
    }
  ).asInstanceOf[Client]
        
  private[this] val logger = LoggerFactory.getLogger(getClass)

  private val indexName = name.toLowerCase
  private val liveIndexName = indexName + ".live"
  private val writeIndexName = indexName + ".write"
  private val docType   = indexName.split('.').takeRight(2).mkString(".")

  private lazy val monitorIndexName = Utils.getProperty("edda", "monitor.collectionName", name, "sys.monitor").get
  private lazy val retentionPolicy = Utils.getProperty("edda.collection", "retentionPolicy", name, "ALL")

  def init() {
    // TODO create index if missing (set replication and shards), add/update mapping
    // TODO create monitorIndexName if missing
  }

  /** perform query on data store, see [[com.netflix.edda.Queryable.query()]] */
  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
    // if query is for "null" ltime, then use the .live index alias
    val builder = 
      if( queryMap.contains("ltime") && queryMap("ltime") == null ) 
        client.prepareSearch().setIndices(liveIndexName).setQuery(esQuery(queryMap - "ltime"))
      else
        client.prepareSearch().setIndices(indexName).setQuery(esQuery(queryMap))
    queryMap.get("id") match {
      case Some(id: String) => builder.setRouting(id)
      case _ =>
    }
    if( !replicaOk ) builder.setPreference("_primary")
    if( limit > 0 ) fetch(builder, limit, keys) else scan(builder, keys)
  }

  /** load records from data store, used at Collection start-up to prime in-memory cache and to refresh
    * in-memory cache when we are not the leader
    *
    * @param replicaOk specify if we can load from a read-replica in the data store when there are
    *                  redundant systems running for high-availability.
    */
  def load(replicaOk: Boolean): Seq[Record] = {
    val builder = client.prepareSearch().setIndices(liveIndexName)
    if( !replicaOk ) builder.setPreference("_primary")
    scan(builder)
  }

  def fetch(search: SearchRequestBuilder, limit: Int, keys: Set[String]): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    import org.elasticsearch.action.search.SearchResponse
    import org.elasticsearch.action.search.SearchType
    val builder = search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).addSort("stime", SortOrder.DESC).setFrom(0).setSize(limit);
    // add fields, but only 2 deep, beyond that we cannot infer the document structure from the response
    if( keys.size > 0 ) builder.addFields((keys + "stime").map(s => s.split('.').take(2).mkString(".")).toSet.toSeq:_*)
    logger.info("["+builder.request.indices.mkString(",")+"]" + " fetch: " + builder.toString)
    val searchResp = builder.execute().actionGet();
    searchResp.getHits().asScala.map(r => {
      try esToRecord(if(keys.size > 0) esFieldsFixup(r.getFields) else r.getSource)
      catch {
        case e: Exception => {
          logger.error(this + " failed to parse record: " + r.getSource, e)
          throw e
        }
      }
    }).toSeq.sortWith((a, b) => a.stime.isAfter(b.stime))
  }

  def scan( search: SearchRequestBuilder, keys: Set[String] = Set()): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    import org.elasticsearch.action.search.SearchResponse
    import org.elasticsearch.action.search.SearchType
    import org.elasticsearch.common.unit.TimeValue
    val builder = search.
      setSearchType(SearchType.SCAN).
      setScroll(new TimeValue(60000)).
      setSize(100)
    // add fields, but only 2 deep, beyond that we cannot infer the document structure from the response
    if( keys.size > 0 ) builder.addFields((keys + "stime").map(s => s.split('.').take(2).mkString(".")).toSet.toSeq:_*)
    logger.info("["+builder.request.indices.mkString(",")+"]" + " scan: " + builder.toString)

    var scrollResp: SearchResponse = builder.execute().actionGet()
    
    //Scroll until no hits are returned
    var keepLooping = true
    var seq: Seq[Record] = Seq()

    while (keepLooping) {
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
      seq = seq ++ scrollResp.getHits().asScala.map(r => {
        try esToRecord(if(keys.size > 0) esFieldsFixup(r.getFields) else r.getSource)
        catch {
          case e: Exception => {
            logger.error(this + " failed to parse record: " + r.getSource, e)
            throw e
          }
        }
      })
      
      //Break condition: No hits are returned
      if (scrollResp.hits().hits().length == 0) {
        keepLooping = false
      }
    }
    seq.sortWith((a, b) => a.stime.isAfter(b.stime))
  }

  
  /** make changes to the data store depending on the Collection delta found after a Crawl result */
  def update(d: Collection.Delta) {
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
  }
  
  def collectionModified: DateTime  = {
    // if query is for "null" ltime, then use the .live index alias
    val response = client.prepareGet(monitorIndexName, "collection.mark", name).setPreference("_primary").execute().actionGet()
    if( response == null || !response.isExists )
      DateTime.now
    else {
      esToRecord(response.getSource).mtime
    }
  }
  
  def markCollectionModified = {
    val markRec = Record(name, Map("updated" -> DateTime.now, "id" -> name, "type" -> "collection"))
    try {
      val response = client.prepareIndex(monitorIndexName, "collection.mark").
        setId(markRec.id).
        setSource(markRec.toString).
        execute().
        actionGet();
      logger.info("index response: " + response.toString)
    } catch {
      case e: Exception => {
        logger.error("failed to index record: " + markRec)
        throw e
      }
    }
  }

  protected def upsert(record: Record) {
    try {
      val response = client.prepareIndex(writeIndexName, docType).
        setId(record.id + "|" + record.stime.getMillis).
        setRouting(record.id).
        setSource(record.toString).
        execute().
        actionGet();
      logger.info("index response: " + response.toString)
    } catch {
      case e: Exception => {
        logger.error("failed to index record: " + record)
        throw e
      }
    }
  }

  protected def remove(record: Record) {
    try {
      val response = client.prepareDelete(writeIndexName, docType, record.id + "|" + record.stime.getMillis).
        setRouting(record.id).
        execute().
        actionGet();
      logger.info("delete response: " + response.toString)
    } catch {
      case e: Exception => {
        logger.error("failed to delete record: " + record)
        throw e
      }
    }
  }

  override def remove(queryMap: Map[String, Any]) {
    try {
      val response = client.prepareDeleteByQuery(writeIndexName).
        setTypes(docType).
        setQuery(esQuery(queryMap)).
        execute().
        actionGet()
    } catch {
      case e: Exception => {
        logger.error("failed to delete records: " + queryMap)
        throw e
      }
    }
  }
    
  override def toString = "[ElasticSearchDatastore " + name + "]"
}
