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

import org.joda.time.DateTime

case class RecordSet(records: Seq[Record] = Seq(), meta: Map[String,Any] = Map())

/** companion Record object to allow easier Record construction */
object Record {
  /** create record with new id and data */
  def apply(id: String, data: Any): Record = {
    val now = DateTime.now
    new Record(
      id = id,
      ftime = now,
      ctime = now,
      stime = now,
      ltime = null,
      mtime = now,
      data = data,
      tags = Map())
  }

  /** create record with new id, create-time and data */
  def apply(id: String, ctime: DateTime, data: Any): Record = {
    val now = DateTime.now
    new Record(
      id = id,
      ftime = now,
      ctime = ctime,
      stime = now,
      ltime = null,
      mtime = now,
      data = data,
      tags = Map())
  }

  /** allows Record to be constructed like a case class */
  def apply(
             id: String,
             ftime: DateTime,
             ctime: DateTime,
             stime: DateTime,
             ltime: DateTime,
             mtime: DateTime,
             data: Any,
             tags: Map[String, Any]) = new Record(id, ftime, ctime, stime, ltime, mtime, data, tags)
}

/** simple record object that can be treated like a case class.
  *
  * @param id name of the resource, unique identifier
  * @param ctime original time the record was created
  * @param stime last state change time
  * @param ltime last seen time (time when record expired)
  * @param mtime last modified time for the record
  * @param data the record data, what is returned by default for the REST interface
  * @param tags arbitrary tags can be applied to the record for internal use
  */
class Record(
              val id: String,
              val ftime: DateTime,
              val ctime: DateTime,
              val stime: DateTime,
              val ltime: DateTime,
              val mtime: DateTime,
              val data: Any,
              val tags: Map[String, Any]) {

  /** copy to behave similar to case class */
  def copy(
            id: String = id,
            ftime: DateTime = ftime,
            ctime: DateTime = ctime,
            stime: DateTime = stime,
            ltime: DateTime = ltime,
            mtime: DateTime = mtime,
            data: Any = data,
            tags: Map[String, Any] = tags) = new Record(id, ftime, ctime, stime, ltime, mtime, data, tags)

  /** flatten object into basic scala map */
  def toMap = {
    Map(
      "id" -> id,
      "ftime" -> ftime,
      "ctime" -> ctime,
      "stime" -> stime,
      "ltime" -> ltime,
      "mtime" -> mtime,
      "data" -> data,
      "tags" -> tags)
  }

  def toId(): String = {
      return (this.id + "|" + this.stime.getMillis)
  }

  /** json serialized string used to compare if 2 records with same id are in-fact identical */
  lazy val dataString = Utils.toJson(this.data)

  /** compare this record to another and return true if the data is identical */
  def sameData(that: Record): Boolean = {
    if (that == null) return false
    this.data == that.data || this.dataString == that.dataString
  }

  /** make logging easier, serialize the Record to json */
  override lazy val toString = Utils.toJson(this.toMap)
}

