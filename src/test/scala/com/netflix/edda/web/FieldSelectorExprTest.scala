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

        val expr = FlattenExpr(FixedExpr(matches=true))

        expect(output) {
            expr.flattenValue(None, input)
        }

        expect(Some(output)) {
            expr.select(input)
        }
    }

    test("KeySelectExpr") {
        val expr1 = KeySelectExpr(Map(
            "a" -> FixedExpr(matches=true),
            "b" -> EqualExpr(42),
            "c" -> EqualExpr("def")
        ))

        val base = Map("b" -> 42, "c" -> "def")

        expect(Some(base)) {
            expr1.select(base)
        }

        expect(Some(base)) {
            expr1.select(base + ("d" -> 65))
        }

        expect(Some(base + ("a" -> 65))) {
            expr1.select(base + ("a" -> 65))
        }

        expect(None) {
            expr1.select(base + ("b" -> 65))
        }

        expect(None) {
            expr1.select(base + ("b" -> "def"))
        }

        expect(Some(List(base))) {
            expr1.select(List(base, base + ("b" -> "def")))
        }

        expect(Some(base + ("a" -> null))) {
            expr1.select(base + ("a" -> null)) 
        }
    }

    test("EqualExpr") {
        expect(Some(42)) {
            EqualExpr(42).select(42)
        }
        expect(None) {
            EqualExpr(42).select(41)
        }
        expect(Some(42)) {
            EqualExpr(42).select(int2Integer(42))
        }
        expect(Some(List(42))) {
            EqualExpr(42).select(List(42,41,40))
        }
        expect(None) {
            EqualExpr(42).select(Map("a" -> 42, "b" -> 0))
        }
    }

    test("NotEqualExpr") {
        expect(None) {
            NotEqualExpr(42).select(42)
        }
        expect(Some(41)) {
            NotEqualExpr(42).select(41)
        }
        expect(None) {
            NotEqualExpr(42).select(int2Integer(42))
        }
        expect(Some(List(41,40))) {
            NotEqualExpr(42).select(List(42,41,40))
        }
        expect(Some(Map("a" -> 42, "b" -> 0))) {
            NotEqualExpr(42).select(Map("a" -> 42, "b" -> 0))
        }
    }

    test("RegexExpr") {
        expect(Some(42)) {
            RegexExpr("42", invert=false).select(42)
        }
        expect(None) {
            RegexExpr("42", invert=true).select(42)
        }
        expect(Some(42)) {
            RegexExpr("[0-9]+", invert=false).select(42)
        }
    }
}
