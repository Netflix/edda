package com.netflix.edda

import org.joda.time.DateTime

import org.slf4j.{Logger, LoggerFactory}

object Record {
    def fromBean(id: String, obj: Any, ctime: DateTime=DateTime.now): Record = {
        val now = DateTime.now
        new Record(
            id=id,
            ctime=ctime,
            stime=now,
            ltime=now,
            mtime=now,
            data="TODO",
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
        tags: Map[String,String]
    ) = new Record(id,ctime,stime,ltime,mtime,data,tags)
}

class Record(
    val id: String,
    val ctime: DateTime,
    val stime: DateTime,
    val ltime: DateTime,
    val mtime: DateTime,
    val data: Any,
    val tags: Map[String,String]
) {
    private val logger = LoggerFactory.getLogger(getClass)

    def copy(
        id: String = id,
        ctime: DateTime = ctime,
        stime: DateTime = stime,
        ltime: DateTime = ltime,
        mtime: DateTime = mtime,
        data: Any=data,
        tags: Map[String,String] = tags
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

    def sameData(that: Record): Boolean = {
        if (that == null) return false
        val ret: Boolean = this.data == that.data || this.data.toString == that.data.toString
        if(!ret) {
            logger.debug("====================> records differ <====================");
            logger.debug("[" + this.data.toString + "," + that.data.toString + "]");
        }
        return ret
        
    }
}

