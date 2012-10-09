package com.netflix.edda.web

import org.scalatest.FunSuite

import FieldSelectorExpr._

class FieldSelectorParserTest extends FunSuite {

    test("ParseSingleKey") {
        val expr = ":(a)"
        expect(KeySelectExpr(Map("a" -> FixedExpr(true)))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseManyKeys") {
        val expr = ":(a,b,c)"
        expect(KeySelectExpr(Map(
                "a" -> FixedExpr(true),
                "b" -> FixedExpr(true),
                "c" -> FixedExpr(true)
            ))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseFlatten") {
        val expr = "::(a,b,c)"
        expect(FlattenExpr(KeySelectExpr(Map(
                "a" -> FixedExpr(true),
                "b" -> FixedExpr(true),
                "c" -> FixedExpr(true)
            )))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseSubExpr") {
        val expr = ":(a,b:(d,e),c::(f,g,h))"
        expect(KeySelectExpr(Map(
                "a" -> FixedExpr(true),
                "b" -> KeySelectExpr(Map(
                    "d" -> FixedExpr(true),
                    "e" -> FixedExpr(true)
                )),
                "c" -> FlattenExpr(KeySelectExpr(Map(
                    "f" -> FixedExpr(true),
                    "g" -> FixedExpr(true),
                    "h" -> FixedExpr(true)
                )))
            ))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseEqualExpr") {
        val expr = ":(a=42)"
        expect(KeySelectExpr(Map("a" -> EqualExpr(42)))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseNotEqualExpr") {
        val expr = ":(a!=42)"
        expect(KeySelectExpr(Map("a" -> NotEqualExpr(42)))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseRegexExpr") {
        val expr = ":(a~/^.*Id$/)"
        expect(KeySelectExpr(Map("a" -> RegexExpr("^.*Id$", false)))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseInvRegexExpr") {
        val expr = ":(a!~/^.*Id$/)"
        expect(KeySelectExpr(Map("a" -> RegexExpr("^.*Id$", true)))) {
            FieldSelectorParser.parse(expr)
        }
    }

    test("ParseStringLiteral") {
        val expr = ":(a=\"42\")"
        expect(KeySelectExpr(Map("a" -> EqualExpr("42")))) {
            FieldSelectorParser.parse(expr)
        }
    }
}
