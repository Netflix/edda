package com.netflix.edda

import org.joda.time.DateTime

trait GroupCollection extends Collection {
    // we dont need to refresh out crawler/collection, it will be 
    // done for us by the grouped collections
    override protected
    def refresher = Unit

    implicit def recordOrdering: Ordering[Record] = Ordering.fromLessThan(_.stime isBefore _.stime)
    
    override
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, keys: Set[String], state: StateMachine.State): Seq[Record] = {
        // if they have specified a subset of keys, then we need to make
        // sure "id" is in there so we can group
        val requiredKeys = if( keys.isEmpty ) keys else (keys + "id")
        val records = super.doQuery(queryMap,limit,live,requiredKeys,state)
        records.groupBy(_.id).values.toSeq.sortBy(_.head).map( mergeRecords(_) )
    }

    def mergeKeys: Map[String,String]

    def mergeRecords(records: Seq[Record]): Record = {
        if( records.size == 1 ) {
            return records.head
        }

        val merge = mergeKeys.map(
            pair => {
                val groupName = pair._1
                val groupKey = pair._2
                var seen: Set[String] = Set()
                groupName -> records.map(
                    rec => {
                        rec.data.asInstanceOf[Map[String,Any]](groupName).asInstanceOf[List[Map[String,Any]]].map(
                            inst => inst ++ Map("end" -> rec.ltime)
                        )
                    }
                ).flatten.filterNot(
                    inst => {
                        val id = inst(groupKey).asInstanceOf[String]
                        val skip = seen.contains(id)
                        if( !skip ) {
                            seen = seen + id
                        }
                        skip
                    }
                )
            }
        ).toMap

        val rec = records.head
        val data = rec.data.asInstanceOf[Map[String,Any]] ++ merge + ("end" -> rec.ltime)
        return rec.copy(data=data)
    }

    def groupSlots(oldRecords: Seq[Record]): Map[String, Map[String,Int]] = {
        mergeKeys.map(
            pair => {
                val groupName = pair._1
                val groupKey  = pair._2
                
                groupName -> oldRecords.flatMap( rec => {
                    rec.data.asInstanceOf[Map[String,Any]](groupName).asInstanceOf[List[Map[String,Any]]].map(
                        item => item(groupKey).asInstanceOf[String] -> item("slot").asInstanceOf[Int] )
                }).toMap
            }
        ).toMap
    }

    def groupDelta(newRecords: Seq[Record], oldRecords: Seq[Record]): Collection.Delta = {
        val oldMap = oldRecords.map( rec => rec.id -> rec).toMap
        val newMap = newRecords.map( rec => rec.id -> rec).toMap
        
        val now=DateTime.now

        val removed = oldRecords.filterNot(
            rec => newMap.contains(rec.id)
        ).map(
            rec => rec.copy(mtime=now,ltime=now)
        )

        val added = newRecords.filterNot( rec => oldMap.contains(rec.id) )
        
        val changes = newRecords.filter( rec => {
            oldMap.contains(rec.id) && newMap.contains(rec.id) && !newMap(rec.id).sameData(oldMap(rec.id))
        }).map( rec => {
            val changes = mergeKeys.filterNot(
                pair => {
                    val groupName = pair._1
                    val groupKey = pair._2
                    
                    // if we have new instances then we increment stime, otherwise just update to new document
                    val newSet = rec.data.asInstanceOf[Map[String,Any]](groupName).asInstanceOf[List[Map[String,Any]]].map(
                        item => item(groupKey).asInstanceOf[String]
                    ).toSet

                    val oldSet = oldMap(rec.id).data.asInstanceOf[Map[String,Any]](groupName).asInstanceOf[List[Map[String,Any]]].map(
                        item => item(groupKey).asInstanceOf[String]
                    ).toSet
                    newSet == oldSet
                }
            )
                
            val oldRec = oldMap(rec.id)
            if( changes.isEmpty ) {
                rec.id -> Collection.RecordUpdate(oldRec, rec.copy(stime=oldRec.stime))
            } else {
                // sets dont have same sets, so create new document revision
                rec.id -> Collection.RecordUpdate(oldRec.copy(mtime=now,ltime=now), rec)
            }
        }).toMap

        // need to reset stime,ctime,tags for crawled records to match what we have in memory
        val fixedRecords = newRecords.collect {
            case rec: Record if changes.contains(rec.id) => {
                val newRec = changes(rec.id).newRecord
                oldMap(rec.id).copy(data=rec.data, mtime=newRec.mtime, stime=newRec.stime)
            }
            case rec: Record if oldMap.contains(rec.id) => 
                oldMap(rec.id).copy(data=rec.data, mtime=rec.mtime)
            case rec: Record => rec
        }

        Collection.Delta(fixedRecords, changed=changes.values.toSeq, added=added, removed=removed)
    }
}
