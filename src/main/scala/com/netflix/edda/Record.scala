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

import org.joda.time.DateTime

object Record {
  def apply(id: String, data: Any): Record = {
    val now = DateTime.now
    new Record(
      id = id,
      ctime = now,
      stime = now,
      ltime = null,
      mtime = now,
      data = data,
      tags = Map())
  }

  def apply(id: String, ctime: DateTime, data: Any): Record = {
    val now = DateTime.now
    new Record(
      id = id,
      ctime = ctime,
      stime = now,
      ltime = null,
      mtime = now,
      data = data,
      tags = Map())
  }

  def apply(
    id: String,
    ctime: DateTime,
    stime: DateTime,
    ltime: DateTime,
    mtime: DateTime,
    data: Any,
    tags: Map[String, Any]) = new Record(id, ctime, stime, ltime, mtime, data, tags)
}

class Record(
  val id: String,
  val ctime: DateTime,
  val stime: DateTime,
  val ltime: DateTime,
  val mtime: DateTime,
  val data: Any,
  val tags: Map[String, Any]) {

  def copy(
    id: String = id,
    ctime: DateTime = ctime,
    stime: DateTime = stime,
    ltime: DateTime = ltime,
    mtime: DateTime = mtime,
    data: Any = data,
    tags: Map[String, Any] = tags) = new Record(id, ctime, stime, ltime, mtime, data, tags)

  def toMap = {
    Map(
      "id" -> id,
      "ctime" -> ctime,
      "stime" -> stime,
      "ltime" -> ltime,
      "mtime" -> mtime,
      "data" -> data,
      "tags" -> tags)
  }

  lazy val dataString = Utils.toJson(this.data)

  def sameData(that: Record): Boolean = {
    if (that == null) return false
    this.data == that.data || this.dataString == that.dataString
  }

  override lazy val toString = Utils.toJson(this.toMap)
}

