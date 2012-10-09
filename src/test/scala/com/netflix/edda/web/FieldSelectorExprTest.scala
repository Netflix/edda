package com.netflix.edda.web

import org.scalatest.FunSuite

import FieldSelectorExpr._

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

        val expr = FlattenExpr(FixedExpr(true))

        expect(output) {
            expr.flattenValue(None, input)
        }

        expect(Some(output)) {
            expr.select(input)
        }
    }

    test("KeySelectExpr") {
        val expr1 = KeySelectExpr(Map(
            "a" -> FixedExpr(true),
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
            RegexExpr("42", false).select(42)
        }
        expect(None) {
            RegexExpr("42", true).select(42)
        }
        expect(Some(42)) {
            RegexExpr("[0-9]+", false).select(42)
        }
    }
}
