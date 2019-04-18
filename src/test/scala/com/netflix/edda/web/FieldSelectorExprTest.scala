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
package com.netflix.edda.web

import org.scalatest.FunSuite

class FieldSelectorExprTest extends FunSuite {

  test("FlattenExpr") {
    val input = Map("b" -> 42, "c" -> Map("d" -> "def", "e" -> List(
      Map("f" -> 1, "g" -> 27),
      Map("f" -> 2, "g" -> 27)
    )))

    val output = Map("b" -> 42, "c.d" -> "def", "c.e" -> List(
      Map("f" -> 1, "g" -> 27),
      Map("f" -> 2, "g" -> 27)
    ))

    val expr = FlattenExpr(FixedExpr(matches = true))

    expectResult(output) {
      expr.flattenValue(None, input)
    }

    expectResult(Some(output)) {
      expr.select(input)
    }
  }

  test("KeySelectExpr") {
    val expr1 = KeySelectExpr(Map(
      "a" -> FixedExpr(matches = true),
      "b" -> EqualExpr(42),
      "c" -> EqualExpr("def")
    ))

    val base = Map("b" -> 42, "c" -> "def")

    expectResult(Some(base)) {
      expr1.select(base)
    }

    expectResult(Some(base)) {
      expr1.select(base + ("d" -> 65))
    }

    expectResult(Some(base + ("a" -> 65))) {
      expr1.select(base + ("a" -> 65))
    }

    expectResult(None) {
      expr1.select(base + ("b" -> 65))
    }

    expectResult(None) {
      expr1.select(base + ("b" -> "def"))
    }

    expectResult(Some(List(base))) {
      expr1.select(List(base, base + ("b" -> "def")))
    }

    expectResult(Some(base + ("a" -> null))) {
      expr1.select(base + ("a" -> null))
    }

    expectResult(Some(base + ("a" -> List(1, 2, 3)))) {
      expr1.select(base + ("a" -> List(1, 2, 3)))
    }

    // FixedExpr(true) matches empty list
    expectResult(Some(base + ("a" -> List()))) {
      expr1.select(base + ("a" -> List()))
    }

    // EqualExpr on list returns empty List if there are not matches
    expectResult(Some(base + ("b" -> List()))) {
      expr1.select(base + ("b" -> List(1, 2, 3)))
    }

    // EqualExpr on list returns just matching items
    expectResult(Some(base + ("b" -> List(42)))) {
      expr1.select(base + ("b" -> List(1, 2, 42)))
    }

    // EqualExpr on empty list return empty list
    expectResult(Some(base + ("b" -> List()))) {
      expr1.select(base + ("b" -> List()))
    }
  }

  test("EqualExpr") {
    expectResult(Some(42)) {
      EqualExpr(42).select(42)
    }
    expectResult(None) {
      EqualExpr(42).select(41)
    }
    expectResult(Some(42)) {
      EqualExpr(42).select(int2Integer(42))
    }
    expectResult(Some(List(42))) {
      EqualExpr(42).select(List(42, 41, 40))
    }
    expectResult(None) {
      EqualExpr(42).select(Map("a" -> 42, "b" -> 0))
    }
  }

  test("NotEqualExpr") {
    expectResult(None) {
      NotEqualExpr(42).select(42)
    }
    expectResult(Some(41)) {
      NotEqualExpr(42).select(41)
    }
    expectResult(None) {
      NotEqualExpr(42).select(int2Integer(42))
    }
    expectResult(Some(List(41, 40))) {
      NotEqualExpr(42).select(List(42, 41, 40))
    }
    expectResult(Some(Map("a" -> 42, "b" -> 0))) {
      NotEqualExpr(42).select(Map("a" -> 42, "b" -> 0))
    }
  }

  test("RegexExpr") {
    expectResult(Some(42)) {
      RegexExpr("42", invert = false).select(42)
    }
    expectResult(None) {
      RegexExpr("42", invert = true).select(42)
    }
    expectResult(Some(42)) {
      RegexExpr("[0-9]+", invert = false).select(42)
    }

    val expr1 = KeySelectExpr(Map(
      "a" -> RegexExpr("^[24]+$", invert = false)
    ))
    expectResult(Some(Map("a" -> 42))) {
      expr1.select(Map("a" -> 42, "b" -> 0))
    }

    val expr2 = KeySelectExpr(Map(
      "a" -> RegexExpr("^[abc]+$", invert = false)
    ))
    expectResult(None) {
      expr2.select(Map("a" -> 42, "b" -> 0))
    }
  }
}
