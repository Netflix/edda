package com.netflix.edda

import org.joda.time.DateTime

import org.slf4j.{Logger, LoggerFactory}

object Record {
    def apply(id: String, data: Any): Record = {
        val now = DateTime.now
        new Record(
            id=id,
            ctime=now,
            stime=now,
            ltime=null,
            mtime=now,
            data=data,
            tags=Map()
        )
    }

    def apply(id: String, ctime: DateTime, data: Any): Record = {
        val now = DateTime.now
        new Record(
            id=id,
            ctime=ctime,
            stime=now,
            ltime=null,
            mtime=now,
            data=data,
            tags=Map()
        )
    }

    def apply(
        id: String,
        ctime: DateTime,
        stime: DateTime,
        ltime: DateTime,
        mtime: DateTime,
        data: Any,
        tags: Map[String,Any]
    ) = new Record(id,ctime,stime,ltime,mtime,data,tags)
}

class Record(
    val id: String,
    val ctime: DateTime,
    val stime: DateTime,
    val ltime: DateTime,
    val mtime: DateTime,
    val data: Any,
    val tags: Map[String,Any]
) {
    import Record._

    private[this] val logger = LoggerFactory.getLogger(getClass)

    def copy(
        id: String = id,
        ctime: DateTime = ctime,
        stime: DateTime = stime,
        ltime: DateTime = ltime,
        mtime: DateTime = mtime,
        data: Any=data,
        tags: Map[String,Any] = tags
    ) = new Record(id,ctime,stime,ltime,mtime,data,tags)

    def toMap = {
        Map(
            "id" -> id,
            "ctime" -> ctime,
            "stime" -> stime,
            "ltime" -> ltime,
            "mtime" -> mtime,
            "data" -> data,
            "tags" -> tags
        )
    }

    lazy val dataString = Utils.toJson(this.data)

    def sameData(that: Record): Boolean = {
        if (that == null) return false
        this.data == that.data || this.dataString == that.dataString
    }
    
    override lazy val toString = Utils.toJson(this.toMap)
}

