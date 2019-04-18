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

class FieldSelectorParserTest extends FunSuite {

  test("ParseSingleKey") {
    val expr = ":(a)"
    expectResult(KeySelectExpr(Map("a" -> FixedExpr(matches = true)))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseManyKeys") {
    val expr = ":(a,b,c)"
    expectResult(KeySelectExpr(Map(
      "a" -> FixedExpr(matches = true),
      "b" -> FixedExpr(matches = true),
      "c" -> FixedExpr(matches = true)
    ))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseFlatten") {
    val expr = "::(a,b,c)"
    expectResult(FlattenExpr(KeySelectExpr(Map(
      "a" -> FixedExpr(matches = true),
      "b" -> FixedExpr(matches = true),
      "c" -> FixedExpr(matches = true)
    )))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseSubExpr") {
    val expr = ":(a,b:(d,e),c::(f,g,h))"
    expectResult(KeySelectExpr(Map(
      "a" -> FixedExpr(matches = true),
      "b" -> KeySelectExpr(Map(
        "d" -> FixedExpr(matches = true),
        "e" -> FixedExpr(matches = true)
      )),
      "c" -> FlattenExpr(KeySelectExpr(Map(
        "f" -> FixedExpr(matches = true),
        "g" -> FixedExpr(matches = true),
        "h" -> FixedExpr(matches = true)
      )))
    ))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseEqualExpr") {
    val expr = ":(a=42)"
    expectResult(KeySelectExpr(Map("a" -> EqualExpr(42)))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseNotEqualExpr") {
    val expr = ":(a!=42)"
    expectResult(KeySelectExpr(Map("a" -> NotEqualExpr(42)))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseRegexExpr") {
    val expr = ":(a~/^.*Id$/)"
    expectResult(KeySelectExpr(Map("a" -> RegexExpr("^.*Id$", invert = false)))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseInvRegexExpr") {
    val expr = ":(a!~/^.*Id$/)"
    expectResult(KeySelectExpr(Map("a" -> RegexExpr("^.*Id$", invert = true)))) {
      FieldSelectorParser.parse(expr)
    }
  }

  test("ParseStringLiteral") {
    val expr = ":(a=\"42\")"
    expectResult(KeySelectExpr(Map("a" -> EqualExpr("42")))) {
      FieldSelectorParser.parse(expr)
    }
  }
}
