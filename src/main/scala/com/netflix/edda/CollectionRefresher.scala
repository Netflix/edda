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
import scala.util.Random

import org.slf4j.LoggerFactory

import org.joda.time.DateTime

class CollectionRefresher(collection: Collection) extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

  override def toString = collection + " refresher"

  lazy val refresh = Utils.getProperty("edda.collection", "refresh", collection.name, "60000")
  lazy val cacheRefresh = Utils.getProperty("edda.collection", "cache.refresh", collection.name, "10000")

  // how often to purge history, default is every 6 hours
  lazy val purgeFrequency = Utils.getProperty("edda.collection", "purgeFrequency", collection.name, "21600000")
  
  /** helper routine to calculate timeLeft before a Crawl request shoudl be made */
  def timeLeft(lastRun: DateTime, millis: Long): Long = {
    val timeLeft = millis - (DateTime.now.getMillis - lastRun.getMillis)
    if (timeLeft < 0) 0 else timeLeft
  }

  override def act() = {
    Actor.self.react {
      case 'CONTINUE => {
        var amLeader = false
        // crawl immediately the first time
        implicit val req = RequestId(s"$this init")
        if (amLeader && collection.allowCrawl) collection.crawler.crawl()

        var lastRun = DateTime.now
        var keepLooping = true
        Actor.self.loopWhile(keepLooping) {
          implicit val req = RequestId(Utils.uuid + " refresh")
          val timeout = if (amLeader) refresh.get.toLong else cacheRefresh.get.toLong
          Actor.self.reactWithin(timeLeft(lastRun, timeout)) {
            case msg @ StateMachine.Stop(from) => keepLooping = false
            case msg @ TIMEOUT => {
              if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} received: $msg")
              if (amLeader) {
                val purge = if( timeLeft(collection.lastPurge, purgeFrequency.get.toLong) > 0 ) false else true
                if( purge ) {
                  val msg = Collection.Purge(Actor.self)
                  if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $collection")
                  collection ! msg
                }
                if( collection.allowCrawl ) collection.crawler.crawl()
              }
                else {
                  val msg = CollectionProcessor.Load(Actor.self)
                  if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $collection")
                  collection.processor ! msg
                }
              lastRun = DateTime.now
            }
            case msg @ Elector.ElectionResult(from, result) => {
              if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} received: $msg from $sender")
              // if we just became leader, then start a crawl
              if (!amLeader && result) {
                val rand = new Random
                // purgeJitter is can be up to +/-20% of purgeFrequency
                val purgeJitter = (purgeFrequency.get.toLong * .2 * rand.nextDouble).toLong * (if( rand.nextBoolean ) 1 else -1);
                val msg = CollectionProcessor.SyncLoad(Actor.self)
                if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $collection")
                collection.processor ! msg
                Actor.self.reactWithin(300000) {
                  case msg @ CollectionProcessor.OK(frm) => {
                    if( collection.allowCrawl ) collection.crawler.crawl()
                    lastRun = DateTime.now
                    amLeader = result
                  }
                  case msg @ TIMEOUT => {
                    if (logger.isErrorEnabled) logger.error(s"$req$collection failed to reload data in 5m as we became leader")
                    throw new java.lang.RuntimeException(s"TIMEOUT: $collection Failed to reload data in 5m as we became leader")
                  }
                }
              } else if( amLeader && !result ) {
                // we have lost the election, so stop updating
                amLeader = result
              } else amLeader = result
            }
          }
        }
      }
    }
  }

  override def exceptionHandler = {
    case e: Exception => if (logger.isErrorEnabled) logger.error(s"$collection failed to refresh", e)
  }
  
  override def start() = {
    if (Option(collection.crawler).isDefined && Option(collection.elector).isDefined) {
      super.start()
      
      implicit val req = RequestId(s"$this start")
      import ObserverExecutionContext._
      collection.elector.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req$this failed to addObserver: $msg")
          start()
        }
        case scala.util.Success(msg) => {
          this ! 'CONTINUE;
        }
      }
    }
    this
  }

  def stop()(implicit req: RequestId) {
    val msg = StateMachine.Stop(Actor.self)
    if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $this")
    this ! msg
  }

}
