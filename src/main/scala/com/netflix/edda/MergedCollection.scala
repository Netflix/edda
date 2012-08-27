package com.netflix.edda

import scala.actors.Futures.{future, awaitAll}

import org.slf4j.{Logger, LoggerFactory}

class MergedCollection(val name: String, val collections: Seq[Collection]) extends Queryable {
    override
    def toString = "[MergedCollection " + name + "]"

    private[this] val logger = LoggerFactory.getLogger(getClass)

    protected
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, state: StateMachine.State): Seq[Record] = {
        val tasks = collections.map(c => future {
            c.query(queryMap,limit,live)
        })
        val records = awaitAll(300000L, tasks:_*) match { 
            case Nil => Seq()
            case x: Seq[Option[Seq[Record]]] => {
                //logger.info("got: " + x)
                x.collect {  case Some(d) => d }.flatten
            }
            case y => {
                //logger.info("got: " + y)
                Seq()
            }
        }
        records.sortWith( (a,b) => a.stime.isAfter(b.stime) )
    }

    override
    def start() = {
        logger.info("Staring "+ this);
        super.start()
    }

    override
    def stop() {
        logger.info("Stoping "+ this);
        super.stop()
    }
}
