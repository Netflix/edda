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
package com.netflix.edda.aws

import com.netflix.edda.Elector
import com.netflix.edda.Utils
import com.netflix.edda.Record
import com.netflix.edda.RequestId

import org.slf4j.LoggerFactory

import org.joda.time.DateTime

/** [[com.netflix.edda.Elector]] subclass that uses DynamoDB's write contstraint operations
  * to organize leadership
  */
class DynamoDBElector extends Elector {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  lazy val instance = Option(
    System.getenv(Utils.getProperty("edda.elector", "uniqueEnvName", "dynamodb", "EC2_INSTANCE_ID").get)).getOrElse("dev")

  val leaderTimeout = Utils.getProperty("edda.elector", "leaderTimeout", "dynamodb", "5000")

  private lazy val monitorTableName = Utils.getProperty("edda.elector", "tableName", "dynamodb", "edda-leader").get

  lazy val readCap   = Utils.getProperty("edda.elector", "readCapacity", "dynamodb", "5").get.toLong
  lazy val writeCap  = Utils.getProperty("edda.elector", "writeCapacity", "dynamodb", "1").get.toLong

  lazy val account = Utils.getProperty("edda", "account", "elector.dynamodb", "").get

  private var inited = false

  val readDynamo = new AwsClient(account).dynamo
  val writeDynamo = {
    val client = new AwsClient(account).dynamo
    client
  }

  override def init() {
    implicit val client = writeDynamo
    DynamoDB.init(monitorTableName, readCap, writeCap)
    inited = true
    super.init()
  }

  /** attempt to become the leader.  If no leader is present it attempts
    * to insert itself as leader (if insert error happens, then someone else became
    * leader before us).  If we are leader then update leader record mtime so that
    * secondary severs see that we are still alive and don't assume leadership.  If
    * we are not leader, double-check the mtime of the record, if it is older than
    * the leaderTimeout value then attempt to update leader record as self.  The records
    * for mtime and new-leader are atomic conditional updates so if some other servers
    * updates dynamodb first we will "lose" will not be the leader.
    * @return
    */
  protected override def runElection()(implicit req: RequestId): Boolean = {
    if( !inited ) {
      return false
    }
    implicit val client = writeDynamo
    val now = DateTime.now
    var leader = instance

    var isLeader = false

    val t0 = System.nanoTime()
    val response = try {
      implicit val client = readDynamo
      DynamoDB.get(monitorTableName, "name", "leader")
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req$this get leader lapse: ${lapse}ms")
    }
    if( response == null || response.isEmpty ) {
      // no record found, so this is the first time we are creating a record
      val t0 = System.nanoTime()
      try {
        DynamoDB.put(
          monitorTableName,
          Map(
            "name" -> "leader",
            "instance" -> instance,
            "mtime" -> DateTime.now.getMillis,
            "req" -> req.id
          ),
          Map(
            // precondition: assert that no record with this name exists
            "name" -> None
          )
        )
        isLeader = true
      } catch {
        case e: Exception => {
          if (logger.isErrorEnabled) logger.error(s"$req$this failed to create leader record: " + e.getMessage)
          isLeader = false
        }
      } finally {
        val t1 = System.nanoTime()
        val lapse = (t1 - t0) / 1000000;
        if (logger.isInfoEnabled) logger.info(s"$req$this create leader lapse: ${lapse}ms")
      }
    } else {
      // record found, if we are leader update mtime
      // if we are not leader check to see if leader record has expired and they try to become leader
      val item = response.get

      leader = item("instance")
      if( leader == instance ) {
        // update mtime
        val t0 = System.nanoTime()
        try {
          DynamoDB.put(
            monitorTableName,
            Map(
              "name" -> "leader",
              "instance" -> instance,
              "mtime" -> DateTime.now.getMillis,
              "req" -> req.id
            ),
            Map(
              // precondition: make sure the update happend on record we fetched
              "instance" -> instance,
              "req" -> item("req")
            )
          )
          isLeader = true
        } catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(s"$req$this failed to update mtime for leader record: ${e.getMessage}")
            isLeader = false
          }
        } finally {
          val t1 = System.nanoTime()
          val lapse = (t1 - t0) / 1000000;
          if (logger.isInfoEnabled) logger.info(s"$req$this index leader (update mtime) lapse: ${lapse}ms")
        }
      } else {
        val mtime = new DateTime(item("mtime").toLong)
        
        val timeout = DateTime.now().plusMillis(-1 * (pollCycle.get.toInt + leaderTimeout.get.toInt))
        if (mtime.isBefore(timeout)) {
          // assume leader is dead, so try to become leader
          val t0 = System.nanoTime()
          try {
            DynamoDB.put(
              monitorTableName,
              Map(
                "name" -> "leader",
                "instance" -> instance,
                "mtime" -> DateTime.now.getMillis,
                "req" -> req.id
              ),
              Map(
                // precondition: make sure the update happend on record we fetched
                "instance" -> leader,
                "req" -> item("req")
              )
            )
            isLeader = true
            leader = instance;
          } catch {
            case e: Exception => {
              if (logger.isErrorEnabled) logger.error(s"$req$this failed to update leader for leader record: ${e.getMessage}")
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
    if (logger.isInfoEnabled) logger.info(s"$req$this Leader [$instance] $isLeader [$leader]")
    isLeader
  }

  override def toString = "[Elector DynamoDB]"
}
