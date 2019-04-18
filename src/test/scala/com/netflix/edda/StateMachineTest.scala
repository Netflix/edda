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
import scala.actors.Futures.{future, awaitAll}

import org.scalatest.FunSuite

case class CounterState(counter: Int = 0)

object Counter extends StateMachine.LocalState[CounterState] {

  case class Inc(from: Actor)(implicit req: RequestId) extends StateMachine.Message

  case class Dec(from: Actor)(implicit req: RequestId) extends StateMachine.Message

  case class Get(from: Actor)(implicit req: RequestId) extends StateMachine.Message

  case class GetResult(from: Actor, result: Int)(implicit req: RequestId) extends StateMachine.Message

}

class Counter extends StateMachine {

  import Counter._

  protected override
  def initState = addInitialState(super.initState, newLocalState(CounterState()))

  def get()(implicit req: RequestId) = this !? Get(this) match {
    case GetResult(from, result) => result
  }

  def inc()(implicit req: RequestId) {
    this ! Inc(this)
  }

  def dec()(implicit req: RequestId) {
    this ! Dec(this)
  }

  private def localTransitions: PartialFunction[(StateMachine.Message, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Inc(from), state) => {
      implicit val req = gotMsg.req
      setLocalState(state, CounterState(localState(state).counter + 1))
    }
    case (Dec(from), state) => {
      setLocalState(state, CounterState(localState(state).counter - 1))
    }
    case (gotMsg @ Get(from), state) => {
      implicit val req = gotMsg.req
      sender ! GetResult(this, localState(state).counter)
      state
    }
  }

  protected override def transitions = localTransitions orElse super.transitions
}

class StateMachineTest extends FunSuite {
  implicit val req = RequestId()
  test("Counter") {
    val counter = new Counter
    counter.start()

    expectResult(1) {
      counter.inc()
      counter.get
    }

    expectResult(0) {
      counter.dec()
      counter.get
    }
  }

  test("Parallel Counter") {
    val counter = new Counter
    counter.start()
    expectResult(1000) {
      val tasks = Range(0, 1000).map(i => future {
        counter.inc()
      })
      awaitAll(3000L, tasks: _*)
      counter.get
    }
    expectResult(0) {
      val tasks = Range(0, 1000).map(i => future {
        counter.dec()
      })
      awaitAll(3000L, tasks: _*)
      counter.get
    }
  }
}

