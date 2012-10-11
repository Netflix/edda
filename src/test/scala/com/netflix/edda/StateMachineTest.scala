package com.netflix.edda

import scala.actors.Actor
import scala.actors.Futures.{ future, awaitAll }

import org.scalatest.FunSuite

case class CounterState(counter: Int = 0)

object Counter extends StateMachine.LocalState[CounterState] {
    case class Inc(from: Actor) extends StateMachine.Message
    case class Dec(from: Actor) extends StateMachine.Message
    case class Get(from: Actor) extends StateMachine.Message
    case class GetResult(from: Actor, result: Int) extends StateMachine.Message
}

class Counter extends StateMachine {
    import Counter._
    protected override
    def initState = addInitialState(super.initState, newLocalState(CounterState()))

    def get() = this !? Get(this) match {
        case GetResult(from, result) => result
    }

    def inc() = this ! Inc(this)
    def dec() = this ! Dec(this)

    private def localTransitions: PartialFunction[(StateMachine.Message, StateMachine.State), StateMachine.State] = {
        case (Inc(from), state) => {
            setLocalState(state, CounterState(localState(state).counter + 1))
        }
        case (Dec(from), state) => {
            setLocalState(state, CounterState(localState(state).counter - 1))
        }
        case (Get(from), state) => {
            sender ! GetResult(this,localState(state).counter)
            state
        }
    }

    protected override def transitions = localTransitions orElse super.transitions
}

class StateMachineTest extends FunSuite {
    test("Counter") {
        val counter = new Counter
        counter.start

        expect(1) {
            counter.inc
            counter.get
        }

        expect(0) {
            counter.dec
            counter.get
        }
    }

    test("Parallel Counter") {
        val counter = new Counter
        counter.start
        expect(0) {
            var tasks = Range(1,1000).map(i => future {counter.inc})
            awaitAll(300000L, tasks: _*)
            tasks = Range(1,1000).map(i => future {counter.dec})
            awaitAll(300000L, tasks: _*)
            counter.get
        }
    }
}

