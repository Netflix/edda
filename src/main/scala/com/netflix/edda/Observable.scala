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

import scala.actors.Actor

case class ObservableState(observers: List[Actor] = List[Actor]())

object Observable extends StateMachine.LocalState[ObservableState] {
  // internal messages
  private case class Observe(from: Actor, actor: Actor) extends StateMachine.Message
  private case class Ignore(from: Actor, actor: Actor) extends StateMachine.Message
  private case class OK(from: Actor) extends StateMachine.Message
}

abstract class Observable extends StateMachine {
  import Observable._

  def addObserver(actor: Actor) {
    this !? (60000, Observe(this, actor)) match {
      case Some(OK(from)) =>
      case Some(message) => throw new java.lang.UnsupportedOperationException("Failed to add observer " + message)
      case None => throw new java.lang.RuntimeException("TIMEOUT: " + this + " Failed to register observer in 60s")
    }
  }

  def delObserver(actor: Actor) {
    this !? (60000, Ignore(this, actor)) match {
      case Some(OK(from)) =>
      case Some(message) => throw new java.lang.UnsupportedOperationException("Failed to remove observer " + message)
      case None => throw new java.lang.RuntimeException("TIMEOUT: " + this + " ailed to unregister observer in 60s")
    }
  }

  protected override def initState = addInitialState(super.initState, newLocalState(ObservableState()))

  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Observe(from, caller), state) => {
      sender ! OK(this)
      setLocalState(state, ObservableState(caller :: localState(state).observers))
    }
    case (Ignore(from, caller), state) => {
      sender ! OK(this)
      setLocalState(state, ObservableState(localState(state).observers diff List(caller)))
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}
