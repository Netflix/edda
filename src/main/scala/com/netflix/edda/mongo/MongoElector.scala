package com.netflix.edda.mongo

import com.netflix.edda.Elector
import com.netflix.edda.DatastoreComponent

import org.slf4j.{Logger, LoggerFactory}

import org.joda.time.DateTime
    
trait MongoElector extends Elector with MongoDatastore {
    import MongoDatastore._

    private[this] val logger = LoggerFactory.getLogger(getClass)

    val instance = System.getenv( config.getProperty("edda.mongo.elector.uniqueEnvName", "EC2_INSTANCE_ID") )
    val name = config.getProperty("edda.mongo.elector.collectionName", "sys.monitor")
    val leaderTimeout = config.getProperty("edda.mongo.elector.leaderTimeout", "5000").toInt

    override
    def init() = {
        super[MongoDatastore].init()
        super.init
    }

    protected override
    def runElection(): Boolean = {
        val now = DateTime.now
        var leader = instance

        var isLeader = false

        val rec = mongo.findOne("leader")
        if( rec == null ) {
            // nobody is leader so try to become leader
            val wr = mongo.insert(
                mapToMongo(
                    Map(
                        "_id" -> "leader",
                        "id" -> "leader",
                        "ctime" -> now,
                        "mtime" -> now,
                        "stime" -> now,
                        "ltime" -> null,
                        "data"  -> Map("instance" -> instance, "id" -> "leader", "type" -> "leader")
                    )
                )
            )
            // if we got an error then uniqueness failed (someone else beat us to it)
	        isLeader = if( wr.getError == null ) true else false
        } else {
            val r = mongoToRecord(rec);
            leader = r.data.asInstanceOf[Map[String,Any]]("instance").asInstanceOf[String];
            val mtime  = r.mtime;
            if( leader == instance ) {
                // update mtime
                val result = mongo.findAndModify(
                    mapToMongo(Map(
                        "_id" -> "leader",
                        "data.instance" -> instance
                    )),   // query
                    null, // sort
                    mapToMongo(Map("mtime" -> now)) // update
                )
                // maybe we were too slow and someone took leader from us
                isLeader = if( result == null ) false else true
            } else {
                val timeout = DateTime.now().plusMillis( -1 * (pollCycle + leaderTimeout))
                if( mtime.isBefore(timeout) ) {
                    // assumer leader is dead, so try to become leader
                    val result = mongo.findAndModify(
                        mapToMongo(Map( // query
                            "_id" -> "leader",
                            "data.instance" -> leader,
                            "mtime" -> mtime
                        )),
                        null,           // sort
                        recordToMongo(  // update
                            r.copy(
                                mtime = now,
                                stime = now,
                                ltime = null,
                                data  = Map("instance" -> instance, "id" -> "leader", "type" -> "leader")
                            ),
                            Some("leader")
                        )
                    )
                    // if we got the update then we are leader and attempt to 
                    // archive the old leader record
                    if( result == null ) {
                        isLeader = false
                    } else {
                        isLeader = true
                        mongo.insert(recordToMongo(r.copy(ltime = now), Some("leader|" + r.stime.getMillis)))
                    }
                } else isLeader = false
            }
        }

        logger.info("Leader [" + instance + "]: " + isLeader + " [" + leader + "]");
        isLeader
    }
}
