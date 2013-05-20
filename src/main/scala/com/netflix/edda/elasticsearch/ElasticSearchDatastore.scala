/**
 * Copyright 2013 Netflix, Inc.
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
import com.netflix.edda.Datastore
import com.netflix.edda.Utils

import org.joda.time.DateTime
import java.util.Date

import org.slf4j.LoggerFactory

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.WriteConsistencyLevel
import org.elasticsearch.action.support.replication.ReplicationType
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.SearchHitField
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.Client
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.rest.RestStatus

// /** helper object to store common ElasticSearch related routines */
object ElasticSearchDatastore {

  import org.joda.time.format.ISODateTimeFormat
  val basicDateTime = ISODateTimeFormat.dateTime
  val basicDateTimeNoMillis = ISODateTimeFormat.dateTimeNoMillis

  /** converts a ElasticSearch source object to a Record */
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

  private val dateTimeNoMillisRx       = """^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$""".r
  private val dateTimeRx = """^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(?:[.]\d\d?\d?)Z$""".r

  private[this] val logger = LoggerFactory.getLogger(getClass)
  /** converts a ElasticSearch java object to a corresponding Scala basic object */
  def esToScala(obj: Any): Any = {
    import collection.JavaConverters._
    obj match {
      case o: java.util.Map[_,_] => {
        o.keySet.asScala.map(key => (key.asInstanceOf[String] -> esToScala(o.get(key)))).toMap
      }
      case o: java.util.Collection[_] => {
        List.empty[Any] ++ o.asScala.map(esToScala(_))
      }
      case dateTimeNoMillisRx() => basicDateTimeNoMillis.parseDateTime(obj.asInstanceOf[String])
      case dateTimeRx() => basicDateTime.parseDateTime(obj.asInstanceOf[String])
      case o: Date => new DateTime(o)
      case o: AnyRef => o
      case null => null
      case other => throw new java.lang.RuntimeException("esToScala: don't know how to handle: " + other)
    }
  }

  def esToJson(rec: Record): String = {
    Utils.toJson(rec.toMap, Utils.dateFormatter)
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

  def initClient(name: String): Client = {
    val settings: Settings = ImmutableSettings.settingsBuilder().put("cluster.name", Utils.getProperty("edda", "elasticsearch.cluster", name, "edda").get).build()
    Utils.getProperty("edda", "elasticsearch.address", name, "edda").get.split(',').fold(new TransportClient(settings))(
      (client, addr) => {
        val parts = addr.asInstanceOf[String].split(':')
        client.asInstanceOf[TransportClient].addTransportAddress(new InetSocketTransportAddress(parts.head, parts.tail.head.toInt))
      }
    ).asInstanceOf[Client]
    // NodeBuilder.nodeBuilder().node().client()
  }

  def createIndex(client: Client, name: String, shards: Int, replicas: Int) {
    val ixClient = client.admin().indices()
    if( ! ixClient.prepareExists(name).execute().actionGet().exists() ) {
      val settings = ImmutableSettings.settingsBuilder().
      put("index.number_of_shards", shards).
      put("index.number_of_replicas",replicas).
      build()
      
      try {
        ixClient.prepareCreate(name).
          setSettings(settings).
          addMapping("_default_", io.Source.fromInputStream(getClass.getResourceAsStream("/elasticsearch/mappings/_default_.json")).mkString).
          execute.
          actionGet
      } catch {
        case e: org.elasticsearch.indices.IndexAlreadyExistsException => Unit // someone already beat us to it, ignore this
      }
    }
  }
}

/** [[com.netflix.edda.Datastore]] subclass that allows ElasticSearch to be used
 *
 * @param name the name of the collection the datastore is for
 */
class ElasticSearchDatastore(val name: String) extends Datastore {

  import Collection.RetentionPolicy._
  import ElasticSearchDatastore._

  lazy val client = initClient(name)
        
  private[this] val logger = LoggerFactory.getLogger(getClass)

  private val lowerName = name.toLowerCase
  private val aliasName = lowerName.replaceAll("[.]", "_");
  private val liveAliasName = aliasName + "_live"
  private val writeAliasName = aliasName + "_write"
  private val docType   = lowerName.split('.').takeRight(2).mkString("_")

  private lazy val monitorIndexName = Utils.getProperty("edda", "monitor.collectionName", "elasticsearch", "sys.monitor").get.replaceAll("[.]","_")
  private lazy val retentionPolicy = Utils.getProperty("edda.collection", "retentionPolicy", name, "ALL")

  private lazy val writeConsistency = WriteConsistencyLevel.fromString( Utils.getProperty("edda", "elasticsearch.writeConsistency", name, "quorum").get )
  private lazy val replicationType  = ReplicationType.fromString( Utils.getProperty("edda", "elasticsearch.replicationType", name, "async").get )

  def init() {
    // we create 1 index for each account.  We version the index (.1) in case we need
    // to add other indexes in the future (in case we run out of room with the first
    // indexes)
    val nameParts = lowerName.split('.')
    val indexName = Utils.getProperty("edda", "elasticsearch.index", name, nameParts.take(nameParts.size - 2).mkString("_") + "_1").get

    createIndex(
      client,
      indexName, 
      Utils.getProperty("edda", "elasticsearch.shards", name, "15").get.toInt,
      Utils.getProperty("edda", "elasticsearch.replicas", name, "2").get.toInt
    )

    val ixClient = client.admin().indices()

    val mapping = io.Source.fromInputStream(getClass.getResourceAsStream("/elasticsearch/mappings/default.json")).mkString
    ixClient.preparePutMapping(indexName).setType(docType).setSource("{\""+docType+"\": " + mapping + "}").setIgnoreConflicts(true).execute.actionGet

    // put new mapping in case it has changed
    // make sure collection alias exists
    if( ! ixClient.prepareExists(aliasName).execute().actionGet().exists() ) {
      ixClient.prepareAliases().addAlias(indexName, aliasName, FilterBuilders.typeFilter(docType)).execute.actionGet
    }

    // make sure live alias exists
    if( ! ixClient.prepareExists(liveAliasName).execute().actionGet().exists() ) {
      ixClient.prepareAliases().addAlias(
        indexName,
        liveAliasName,
        FilterBuilders.andFilter(
          FilterBuilders.typeFilter(docType),
          FilterBuilders.missingFilter("ltime").nullValue(true).existence(true)
        )
      ).execute.actionGet
    }
    
    // make sure the write alias exists
    if( ! ixClient.prepareExists(writeAliasName).execute().actionGet().exists() ) {
      ixClient.prepareAliases().addAlias(indexName, writeAliasName).execute.actionGet
    }
  }

  /** perform query on data store, see [[com.netflix.edda.Queryable.query()]] */
  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
    // if query is for "null" ltime, then use the .live index alias
    val builder = 
      if( queryMap.contains("ltime") && queryMap("ltime") == null ) 
        client.prepareSearch().setIndices(liveAliasName).setQuery(esQuery(queryMap - "ltime"))
      else
        client.prepareSearch().setIndices(aliasName).setQuery(esQuery(queryMap))
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
    val builder = client.prepareSearch().setIndices(liveAliasName)
    if( !replicaOk ) builder.setPreference("_primary")
    scan(builder)
  }

  def fetch(search: SearchRequestBuilder, limit: Int, keys: Set[String]): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    import org.elasticsearch.action.search.SearchResponse
    import org.elasticsearch.action.search.SearchType
    val builder = search.setTypes(docType).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).addSort("stime", SortOrder.DESC).setFrom(0).setSize(limit);
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
      setTypes(docType).
      setSearchType(SearchType.SCAN).
      setScroll(new TimeValue(60000)).
      setSize(1000)
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
    
    if( Collection.RetentionPolicy.withName(retentionPolicy.get) == LIVE ) {
      val purge = records.filter(_.ltime != null)
      val updating = records.filter(_.ltime == null)
      remove(purge ++ toRemove)
      upsert(records)
    } else {
      remove(toRemove)
      upsert(records)
    }
    markCollectionModified
  }
  
  override def collectionModified: DateTime  = {
    // if query is for "null" ltime, then use the .live index alias
    val response = client.prepareGet(monitorIndexName, "collection_mark", name).setPreference("_primary").execute().actionGet()
    if( response == null || !response.isExists )
      DateTime.now
    else {
      esToRecord(response.getSource).mtime
    }
  }
  
  def markCollectionModified = {
    val markRec = Record(name, Map("updated" -> DateTime.now, "id" -> name, "type" -> "collection"))
    try {
      client.prepareIndex(monitorIndexName, "collection_mark").
        setId(markRec.id).
        setSource(esToJson(markRec)).
        setConsistencyLevel(writeConsistency).
        setReplicationType(replicationType).
        execute().
        actionGet();
    } catch {
      case e: Exception => {
        logger.error("failed to index record: " + markRec, e)
        throw e
      }
    }
  }

  protected def upsert(record: Record) {
    try {
      client.prepareIndex(writeAliasName, docType).
        setId(record.id + "|" + record.stime.getMillis).
        setRouting(record.id).
        setSource(esToJson(record)).
        setConsistencyLevel(writeConsistency).
        setReplicationType(replicationType).
        execute().
        actionGet();
    } catch {
      case e: Exception => {
        logger.error("failed to index record: " + record, e)
        throw e
      }
    }
  }

  protected def upsert(records: Seq[Record]) {
    try {
      records.sliding(1000).foreach( recs => {
        val bulk = client.prepareBulk
        recs.foreach( rec => {
          bulk.add(
            client.prepareIndex(writeAliasName, docType).
              setId(rec.id + "|" + rec.stime.getMillis).
              setRouting(rec.id).
              setSource(esToJson(rec)).
              setConsistencyLevel(writeConsistency).
              setReplicationType(replicationType)
          )
        })
        val response = bulk.execute.actionGet
        if( response.hasFailures() ) {
          val err = this + " failed to bulk index: " + response.buildFailureMessage()
          logger.error(err)
          throw new java.lang.RuntimeException(err)
        }
      })
    }
    catch {
      case e: Exception => {
        logger.error("failed to bulk index records", e)
      }
    }
  }

  protected def remove(record: Record) {
    try {
      val response = client.prepareDelete(writeAliasName, docType, record.id + "|" + record.stime.getMillis).
        setRouting(record.id).
        execute().
        actionGet();
      if( response.isNotFound() ) {
        logger.error(this + " failed to delete \"" + record.id + "|" + record.stime.getMillis + "\": Not Found")
      }
    } catch {
      case e: Exception => {
        logger.error("failed to delete record: " + record, e)
        throw e
      }
    }
  }

  override def remove(queryMap: Map[String, Any]) {
    try {
      val response = client.prepareDeleteByQuery(writeAliasName).
        setTypes(docType).
        setQuery(esQuery(queryMap)).
        execute().
        actionGet()
      // FIXME need to upgrade elasticsearch so that DeleteByQueryResponse has status() member
      // if( response.status() != RestStatus.OK ) {
      //   val err = this + " failed to delete with query " + queryMap.toString
      //   logger.error(err)
      //   throw new java.lang.RuntimeException(err)
      // }
    } catch {
      case e: Exception => {
        logger.error("failed to delete records: " + queryMap, e)
        throw e
      }
    }
  }

  protected def remove(records: Seq[Record]) {
    try {
      records.sliding(1000).foreach( recs => {
        val bulk = client.prepareBulk
        recs.foreach( rec => {
          bulk.add(
            client.prepareDelete(writeAliasName, docType, rec.id + "|" + rec.stime.getMillis).setRouting(rec.id)
          )
        })
        val response = bulk.execute.actionGet
        if( response.hasFailures() ) {
          logger.error(this + " failed to bulk delete: " + response.buildFailureMessage())
        }
      })
    }
    catch {
      case e: Exception => {
        logger.error("failed to bulk index records", e)
      }
    }
  }
    
  override def toString = "[ElasticSearchDatastore " + name + "]"
}
