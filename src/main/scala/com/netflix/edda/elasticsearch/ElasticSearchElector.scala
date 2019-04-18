/*
 * Copyright 2012-2019 Netflix, Inc.
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
package com.netflix.edda.elasticsearch

import com.netflix.edda.Elector
import com.netflix.edda.Utils
import com.netflix.edda.Record
import com.netflix.edda.RequestId

import org.slf4j.LoggerFactory

import org.joda.time.DateTime

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.action.WriteConsistencyLevel
import org.elasticsearch.action.support.replication.ReplicationType

/** [[com.netflix.edda.Elector]] subclass that uses ElasticSearch's versioned write operations
  * to organize leadership
  */
class ElasticSearchElector extends Elector {
private[this] val logger = LoggerFactory.getLogger(getClass)
  import ElasticSearchDatastore._

  lazy val client = initClient("elector.elasticsearch")

  lazy val instance = Option(
    System.getenv(Utils.getProperty("edda.elector", "uniqueEnvName", "elasticsearch", "EC2_INSTANCE_ID").get)).getOrElse("dev")

  val leaderTimeout = Utils.getProperty("edda.elector", "leaderTimeout", "elasticsearch", "5000")

  private lazy val monitorIndexName = Utils.getProperty("edda", "monitor.collectionName", "elasticsearch", "sys.monitor").get.replaceAll("[.]","_")

  private val writeConsistencyProp = Utils.getProperty("edda", "elasticsearch.writeConsistency", monitorIndexName, "quorum")
  private def writeConsistency = WriteConsistencyLevel.fromString( writeConsistencyProp.get )
  private val replicationTypeProp = Utils.getProperty("edda", "elasticsearch.replicationType", monitorIndexName, "async")
  private def replicationType  = ReplicationType.fromString( replicationTypeProp.get )

  private val docType = "leader"

  private var inited = false
  override def init() {
    // make sure the sys.monitor index exists, there is no redundancy or sharding
    // for the monitor collection, it is only used for leadership election
    // and for tracking the modifiedTimes per collection
    createIndex(
      client,
      monitorIndexName,
      shards=1,
      Utils.getProperty("edda", "elasticsearch.replicas", monitorIndexName, "2").get.toInt
    )
    inited = true
    super.init()
  }

  /** select the leader record from ElasticSearch to determine if we are the leader */
  override
  def isLeader()(implicit req: RequestId): Boolean = {
    if( inited ) {
      val response = client.prepareGet(monitorIndexName, docType, "leader").setPreference("_primary").execute().actionGet()
      if( response != null && response.isExists ) {
        esToRecord(response.getSource).data.asInstanceOf[Map[String,Any]]("instance").asInstanceOf[String] == instance
      } else false
    }
    else false
  }


  /** attempt to become the leader.  If no leader is present it attempts
    * to insert itself as leader (if insert error happens, then someone else became
    * leader before us).  If we are leader then update leader record mtime so that
    * secondary severs see that we are still alive and don't assume leadership.  If
    * we are not leader, double-check the mtime of the record, if it is older than
    * the leaderTimeout value then attempt to update leader record as self.  The records
    * for mtime and new-leader are atomic conditional updates so if some other servers
    * updates elasticsearch first we will "lose" will not be the leader.
    * @return
    */
  protected override def runElection()(implicit req: RequestId): Boolean = {
    if( !inited ) {
      return false
    }

    val now = DateTime.now
    var leader = instance

    var isLeader = false

    val t0 = System.nanoTime()
    val response = try {
      client.prepareGet(monitorIndexName, docType, "leader").setPreference("_primary").execute().actionGet()
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req$this get leader lapse: ${lapse}ms")
    }
    if( response == null || !response.isExists ) {
      val t0 = System.nanoTime()
      try {
        val leaderRec = Record("leader", Map("instance" -> instance, "id" -> "leader", "type" -> docType))
        // FIXME what error do I get when the document already exists?
        client.prepareIndex(monitorIndexName, docType).
          setId("leader").
          setSource(esToJson(leaderRec)).
          setConsistencyLevel(writeConsistency).
          setReplicationType(replicationType).
          setCreate(true).
          execute().
          actionGet();
          isLeader = true
      } catch {
        case e: Exception => {
          if (logger.isErrorEnabled) logger.error(s"$req failed to create leader record: ${e.getMessage}")
          isLeader = false
        }
      } finally {
        val t1 = System.nanoTime()
        val lapse = (t1 - t0) / 1000000;
        if (logger.isInfoEnabled) logger.info(s"$req$this create leader lapse: ${lapse}ms")
      }
    } else {
      val leaderRec = esToRecord(response.getSource)
      leader = leaderRec.data.asInstanceOf[Map[String, Any]]("instance").asInstanceOf[String]
      val mtime = leaderRec.mtime
      if( leader == instance ) {
        // update mtime
        val t0 = System.nanoTime()
        try {
          client.prepareIndex(monitorIndexName, docType).
            setId("leader").
            setSource(esToJson(leaderRec.copy(mtime=now))).
            setConsistencyLevel(writeConsistency).
            setReplicationType(replicationType).
            setVersion(response.getVersion).
            setCreate(false).
            execute().
            actionGet()
          isLeader = true
        } catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(s"$req failed to update mtime for leader record: ${e.getMessage}")
            isLeader = false
          }
        } finally {
          val t1 = System.nanoTime()
          val lapse = (t1 - t0) / 1000000;
          if (logger.isInfoEnabled) logger.info(s"$req$this index leader (update mtime) lapse: ${lapse}ms")
        }
      } else {
        val timeout = DateTime.now().plusMillis(-1 * (pollCycle.get.toInt + leaderTimeout.get.toInt))
        if (mtime.isBefore(timeout)) {
          // assume leader is dead, so try to become leader
          val t0 = System.nanoTime()
          try {
            client.prepareIndex(monitorIndexName, docType).
              setId("leader").
              setSource(esToJson(leaderRec.copy(mtime=now, stime=now, data=leaderRec.data.asInstanceOf[Map[String,Any]] + ("instance" -> instance)))).
              setConsistencyLevel(writeConsistency).
              setReplicationType(replicationType).
              setVersion(response.getVersion).
              setCreate(false).
              execute().
              actionGet()
            isLeader = true
            leader = instance;
            // old leader is gone, so create historical record from old leader record
            client.prepareIndex(monitorIndexName, docType).
              setId("leader|" + leaderRec.stime.getMillis).
              setSource(esToJson(leaderRec.copy(ltime=now))).
              setConsistencyLevel(writeConsistency).
              setReplicationType(replicationType).
              setCreate(true).
              execute().
              actionGet()
          } catch {
            case e: Exception => {
              if (logger.isErrorEnabled) logger.error(s"$req failed to update leader for leader record: ${e.getMessage}")
              isLeader = false
            }
          } finally {
            val t1 = System.nanoTime()
            val lapse = (t1 - t0) / 1000000;
            if (logger.isInfoEnabled) logger.info(s"$req$this index leader + archive old leader lapse: ${lapse}ms")
          }
        }
      }
    }
    if (logger.isInfoEnabled) logger.info(s"$req Leader [$instance]: $isLeader [$leader]")
    isLeader
  }

  override def toString = "[Elector ElasticSearch]"
}
