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
package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import org.joda.time.DateTime
import org.slf4j.Logger

trait GroupCollection extends Collection {

  def logger: Logger

  // we only need to refresh cache if we are not leader.  When we are leader
  // then we will get events from the crawler
  override protected def refresher() {
    if (Option(crawler) == None || Option(elector) == None) return
    val cacheRefresh = Utils.getProperty(ctx.config, "edda.collection", "cache.refresh", name, "10000").toLong
    Utils.NamedActor(this + " refresher") {
      elector.addObserver(Actor.self)
      var amLeader = elector.isLeader
      var lastRun = DateTime.now
      Actor.loop {
        val timeout = cacheRefresh
        Actor.reactWithin(timeLeft(lastRun, timeout)) {
          case TIMEOUT => {
            if (!amLeader) this ! Collection.Load(this)
            lastRun = DateTime.now
          }
          case Elector.ElectionResult(from, result) => {
            amLeader = result
          }
          case message => {
            logger.error("Invalid message " + message + " from sender " + sender)
          }
        }
      }
    }
  }

  implicit def recordOrdering: Ordering[Record] = Ordering.fromLessThan(_.stime isBefore _.stime)

  override def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], state: StateMachine.State): Seq[Record] = {
    // if they have specified a subset of keys, then we need to make
    // sure "id" is in there so we can group
    val requiredKeys = if (keys.isEmpty) keys else (keys + "id")
    val records = super.doQuery(queryMap, limit, live, requiredKeys, state)
    if (keys.isEmpty || mergeKeys.find(pair => keys.contains(pair._1)) != None) {
      records.groupBy(_.id).values.toSeq.sortBy(_.head).map(mergeRecords(_))
    } else if (keys.contains("end")) {
      records.map(rec => {
        val data = rec.data.asInstanceOf[Map[String, Any]] + ("end" -> rec.ltime)
        rec.copy(data = data)
      })
    } else records
  }

  def mergeKeys: Map[String, String]

  def mergeRecords(records: Seq[Record]): Record = {
    if (records.size == 1) {
      return records.head
    }

    val merge = mergeKeys.map(
      pair => {
        val groupName = pair._1
        val groupKey = pair._2
        var seen: Set[String] = Set()
        groupName -> records.map(
          rec => {
            rec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[List[Map[String, Any]]].map(
              inst => inst ++ Map("end" -> rec.ltime))
          }).flatten.filterNot(
            inst => {
              val id = inst(groupKey).asInstanceOf[String]
              val skip = seen.contains(id)
              if (!skip) {
                seen = seen + id
              }
              skip
            })
      }).toMap

    val rec = records.head
    val data = rec.data.asInstanceOf[Map[String, Any]] ++ merge + ("end" -> rec.ltime)
    rec.copy(data = data)
  }

  def groupSlots(oldRecords: Seq[Record]): Map[String, Map[String, Int]] = {
    mergeKeys.map(
      pair => {
        val groupName = pair._1
        val groupKey = pair._2

        groupName -> oldRecords.flatMap(rec => {
          rec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[List[Map[String, Any]]].map(
            item => item(groupKey).asInstanceOf[String] -> item("slot").asInstanceOf[Int])
        }).toMap
      }).toMap
  }

  def assignSlots(group: Seq[Map[String, Any]], groupKey: String, slotMap: Map[String, Int]): Seq[Map[String, Any]] = {

    val usedSlots: Set[Int] = group.map(
      item => item(groupKey).asInstanceOf[String]).collect({
        case id: String if slotMap.contains(id) => slotMap(id)
      }).toSet

    var unusedSlots = Range(0, group.size).collect {
      case slot if !usedSlots.contains(slot) => slot
    }

    group.map(
      item => {
        val id = item(groupKey).asInstanceOf[String]
        val slot = slotMap.get(id) match {
          case Some(s) => s
          case None => {
            val slot = unusedSlots.head
            unusedSlots = unusedSlots.tail
            slot
          }
        }
        item + ("slot" -> slot)
      }).sortWith((a, b) => a("slot").asInstanceOf[Int] < b("slot").asInstanceOf[Int])
  }

  def groupDelta(newRecords: Seq[Record], oldRecords: Seq[Record]): Collection.Delta = {
    val oldMap = oldRecords.map(rec => rec.id -> rec).toMap
    val newMap = newRecords.map(rec => rec.id -> rec).toMap

    val now = DateTime.now

    val removed = oldRecords.filterNot(
      rec => newMap.contains(rec.id)).map(
        rec => rec.copy(mtime = now, ltime = now))

    val added = newRecords.filterNot(rec => oldMap.contains(rec.id))

    val changes = newRecords.filter(rec => {
      oldMap.contains(rec.id) && newMap.contains(rec.id) && !newMap(rec.id).sameData(oldMap(rec.id))
    }).map(rec => {
      val changes = mergeKeys.filterNot(
        pair => {
          val groupName = pair._1
          val groupKey = pair._2

          // if we have new instances then we increment stime, otherwise just update to new document
          val newSet = rec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[List[Map[String, Any]]].map(
            item => item(groupKey).asInstanceOf[String]).toSet

          val oldSet = oldMap(rec.id).data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[List[Map[String, Any]]].map(
            item => item(groupKey).asInstanceOf[String]).toSet
          newSet == oldSet
        })

      val oldRec = oldMap(rec.id)
      if (changes.isEmpty) {
        rec.id -> Collection.RecordUpdate(oldRec, rec.copy(stime = oldRec.stime))
      } else {
        // sets don't have same sets, so create new document revision
        rec.id -> Collection.RecordUpdate(oldRec.copy(mtime = now, ltime = now), rec)
      }
    }).toMap

    // need to reset stime, ctime, tags for crawled records to match what we have in memory
    val fixedRecords = newRecords.collect {
      case rec: Record if changes.contains(rec.id) => {
        val newRec = changes(rec.id).newRecord
        oldMap(rec.id).copy(data = rec.data, mtime = newRec.mtime, stime = newRec.stime)
      }
      case rec: Record if oldMap.contains(rec.id) =>
        oldMap(rec.id).copy(data = rec.data, mtime = rec.mtime)
      case rec: Record => rec
    }

    Collection.Delta(fixedRecords, changed = changes.values.toSeq, added = added, removed = removed)
  }
}
