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
package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import org.joda.time.DateTime
import org.slf4j.Logger

/** trait to add grouping behavior to a collection.  /group apis are special in that
  * they don't track detailed history for the resources.  Only when group membership
  * changes do we create a new document revision.
  */
trait GroupCollection extends Collection {

  def logger: Logger

  import Utils._

  /**
    * Crawl events as a secondary result of other Collections/Crawlers running.  For example
    * we have group.autoScalingGroups collection which modified results from the aws.autoScalingGroups
    * Crawler.  If aws.autoScalingGroups crawler is not run or not enabled, then the group collection will be stale.
    */
  override protected[edda] def allowCrawl = false

  implicit def recordOrdering: Ordering[Record] = Ordering.fromLessThan(_.stime isBefore _.stime)
  implicit def timeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isAfter _)

  /** special query where we merge multiple query results for the same record ids into one record.  For
    * autoScalingGroups there will be "instances", if there are many revisions to these instances we want
    * to return a single document for all the revisions with the lastest revision of every instance in the queryied
    * timeframe.  See [[com.netflix.edda.Queryable.query]]
    */
  override def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean, state: StateMachine.State)(implicit req: RequestId): Seq[Record] = {
    // if they have specified a subset of keys, then we need to make
    // sure "id" is in there so we can group
    val requiredKeys = if (keys.isEmpty) keys else (keys + "id")
    val records = super.doQuery(queryMap, limit, live, requiredKeys, replicaOk, state)

    if (keys.isEmpty || mergeKeys.find(pair => keys.contains("data." + pair._1)) != None) {
      records.groupBy(_.id).values.toSeq.sortBy(_.head).map((recs: Seq[Record]) => mergeRecords(recs.sortBy(_.stime)))
    } else if (keys.contains("data.end")) {
      records.map(rec => {
        val data = rec.data.asInstanceOf[Map[String, Any]] + ("end" -> rec.ltime)
        rec.copy(data = data)
      })
    } else records
  }

  /** abstract interface to specify which resource keys we need to merge together.  For autoScalingGroups
    * this would be Map("instances" -> "instanceId")
    */
  def mergeKeys: Map[String, String]

  /** merge keys of multiple records into one record.  For autoScalingGroups we merge
    * all the instances for multiple records into one big instances Map and we make then
    * instances unique based on the mergeKey (instanceId).  We only show the most recent
    * resource revision (only latest revision of an instance from multiple autoScalingGroup
    * records).
    */
  def mergeRecords(records: Seq[Record]): Record = {
    val merge = mergeKeys.map(
      pair => {
        val groupName = pair._1
        val groupKey = pair._2
        var seen: Set[String] = Set()
        groupName -> records.map(
          rec => {
            rec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[Seq[Map[String, Any]]].map(
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

  /** for each mergeKey, go through the oldRecords to determine pre-existing slot assignments
    * and return a map so the slot assignments can be reused
    *
    * @param oldRecords records that will have original slot ids so we can preserve slot assignments
    *                   over multiple crawls
    */
  def groupSlots(oldRecords: Seq[Record]): Map[String, Map[String, Int]] = {
    mergeKeys.map(
      pair => {
        val groupName = pair._1
        val groupKey = pair._2

        groupName -> oldRecords.flatMap(rec => {
          rec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[Seq[Map[String, Any]]].map(
            item => item(groupKey).asInstanceOf[String] -> item("slot").asInstanceOf[Int])
        }).toMap
      }).toMap
  }

  /** assign a numeric slot number for all items that we merge. If there is already a slot
    * number, then reuse it.  If there is no slot then use first unassigned number (starting
    * at 0)
    *
    * @param group group of resources to assign slots to (list of instances)
    * @param groupKey primary key for the group of resources (instanceId)
    * @param slotMap map of resources still active with current slot assignments
    * @return list of resources modified to have a "slot" key
    */
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

  /** only create new document revision if the membership of the groups have changed.
    * If an instance is added or removed from an autoScalingGroup then we return true
    * to create a new document revision, otherwise we return false so that
    * the previous document will get overwritten.  If an instance change state from Pending to Running
    * then the document will just get updated and no new revision will get created.
    */
  override protected
  def newStateTimeForChange(newRec: Record, oldRec: Record): Boolean = {
    val changes = mergeKeys.filterNot(
      pair => {
        val groupName = pair._1
        val groupKey = pair._2

        // if we have new instances then we increment stime, otherwise just update to new document
        val newSet = newRec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[Seq[Map[String, Any]]].map(
          item => item(groupKey).asInstanceOf[String]).toSet

        val oldSet = oldRec.data.asInstanceOf[Map[String, Any]](groupName).asInstanceOf[Seq[Map[String, Any]]].map(
          item => item(groupKey).asInstanceOf[String]).toSet
        newSet == oldSet
      }
    )
    !changes.isEmpty
  }
}
