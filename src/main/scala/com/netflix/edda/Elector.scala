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

import java.util.concurrent.Callable
import java.util.concurrent.ThreadPoolExecutor

import com.netflix.servo.monitor.Monitors
import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge
import com.netflix.servo.DefaultMonitorRegistry

import org.slf4j.LoggerFactory

/** state for Elector StateMachine
  *
  * @param isLeader boolean flag to indicate if we are leader
  */
case class ElectorState(isLeader: Boolean = false)

/** companion object for [[com.netflix.edda.Elector]]. */
object Elector extends StateMachine.LocalState[ElectorState] {

  val logger = LoggerFactory.getLogger(getClass)

  /** Message sent to observers after an Election */
  case class ElectionResult(from: Actor, result: Boolean)(implicit req: RequestId) extends StateMachine.Message

  /** Message to start an election and decide leadership */
  case class RunElection(from: Actor)(implicit req: RequestId) extends StateMachine.Message

  /** message to get the leadership status */
  case class IsLeader(from: Actor)(implicit req: RequestId) extends StateMachine.Message
}

/** interface for determining leadership in a multi system configuration.
  */

abstract class Elector extends Observable {

  import Elector._
  import Utils._

  val self = this
  private[this] val leaderGauge: BasicGauge[java.lang.Integer] = new BasicGauge[java.lang.Integer](
    MonitorConfig.builder("leader").build(),
    new Callable[java.lang.Integer] {
      def call() = if (self.isLeader()(RequestId("leaderGauge"))) 1 else 0
    })

  /** synchronous call to the StateMachine to fetch the leadership status */
  val duration = scala.concurrent.duration.Duration(
    10000,
    scala.concurrent.duration.MILLISECONDS
  )

  def isLeader()(implicit req: RequestId): Boolean = {
    import ElectorExecutionContext._
    val p = scala.concurrent.promise[Boolean]
    Utils.namedActor(this + " elector client") {
      val msg = IsLeader(Actor.self)
      if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $this with 10000ms timeout")
      this !?(10000, msg) match {
        case Some(ElectionResult(from, result)) => p success result
        case Some(message) => p failure new java.lang.UnsupportedOperationException("Failed to determine leadership: " + message)
        case None => p failure new java.lang.RuntimeException("TIMEOUT: isLeader response within 10s")
      }
    }
    scala.concurrent.Await.result(p.future, duration)
  }

  val pollCycle = Utils.getProperty("edda.elector", "refresh", "", "10000")

  val electionPoller = new ElectorPoller(this)

  /** abstract method to determine leadership */
  protected def runElection()(implicit req: RequestId): Boolean

  /** setup initial Elector state */
  protected override def initState = addInitialState(super.initState, newLocalState(ElectorState()))

  /** set up observers to watch ourselves for leadership results.  Start an election immediately
    * then start the poller that will periodically run elections. */
  protected override def init() {
    implicit val req = RequestId("init")
    Monitors.registerObject("edda.elector", this)
    DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor("edda.elector.threadpool", this.pool.asInstanceOf[ThreadPoolExecutor]))
    Utils.namedActor(this + " init") {
      val msg = RunElection(Actor.self)
      if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $this")
      this ! msg
      electionPoller.start()
      super.init
      // listen to our own message events
      def retry: Unit = {
        import ObserverExecutionContext._
        this.addObserver(this) onFailure {
          case msg => {
            if (logger.isErrorEnabled) logger.error(s"$req${Actor.self} failed to add observer $this to $this, retrying")
            retry
          }
        }
      }
      retry
    }
  }

  /** handle StateMachine messages */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ RunElection(from), state) => {
      implicit val req = gotMsg.req
      flushMessages {
        case RunElection(from) => true
      }
      // Utils.NamedActor(this + " election runner") {
      import ElectorExecutionContext._
      scala.concurrent.future {
        val result = runElection()
        val msg = ElectionResult(this, result)
        Observable.localState(state).observers.foreach(o => {
            if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $o")
            o ! msg
        })
      }
      state
    }
    case (ElectionResult(from, result), state) => {
      setLocalState(state, ElectorState(result))
    }
    case (gotMsg @ IsLeader(from), state) => {
      implicit val req = gotMsg.req
      val msg = ElectionResult(this, localState(state).isLeader)
      if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $sender")
      sender ! msg
      state
    }
  }

  override def stop()(implicit req: RequestId) {
    electionPoller.stop()
    super.stop()
  }

  override def transitions = localTransitions orElse super.transitions
}

