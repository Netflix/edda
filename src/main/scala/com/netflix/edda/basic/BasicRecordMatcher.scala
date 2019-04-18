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
package com.netflix.edda.basic

import com.netflix.edda.RecordMatcher

import java.util.Date
import org.joda.time.DateTime

/** This class allows advanced query options against in-memory records. */

class BasicRecordMatcher extends RecordMatcher {
  /** apply query rules against in-memory records
    *
    * The query format is a Map, where the keys follow the MongoDB syntax:
    * http://www.mongodb.org/display/DOCS/Advanced+Queries
    *
    * @param queryMap key/values for the matching requirements
    * @param record the record match against
    * @return true if the record matches
    */
  def doesMatch(queryMap: Map[String, Any], record: Map[String, Any]): Boolean = {
    // find the first rule where rule does not match record
    !queryMap.exists(!matchRule(_, record))
  }

  /** Match a specific rule from the queryMap
   *
   * @param rule a key/value pair specifying the matching rule
   * @param record the record to match against
   * @return true if the record matches the rule
   */
  protected def matchRule(rule: Any, record: Map[String, Any]): Boolean = {
    rule match {
      case map: Map[_, _] => !map.exists(!matchRule(_, record))
      // { key: { $op1: val, $op2: val } } ==>
      // { key: { $op1: val }, key: { $op2: val } {
      case (key: String, value: Map[_, _]) => !value.exists(subrule => !matchRule((key -> subrule), record))
      // { $or: [ {key: value}, {key: value} ] }
      case ("$or", value: Seq[_]) => value.exists(matchRule(_, record))
      // { $and: [ {key: value}, {key: value} ] }
      case ("$and", value: Seq[_]) => !value.exists(!matchRule(_, record))
      case (key: String, (op: String, value: Any)) => matchOp(key, value, op, record)
      case (key: String, value: Any) => matchOp(key, value, "$eq", record)
      case (key: String, null) => matchOp(key, null, "$eq", record)
    }
  }

  /** basic comparator that attempts to do "the right thing" when passed
    * various simple data types.  There is probably a better way to do it.*/
  protected def cmpPartialMatcher: PartialFunction[(Any, Any), Int] = {
    case (null, null) => 0
    case (found, null) => -1
    case (null, expected) => 1

    case (found: Boolean, expected: String) => found.compareTo(expected.toBoolean)
    case (found: String, expected: Boolean) => found.toBoolean.compareTo(expected)
    case (found: Boolean, expected: Boolean) => found.compareTo(expected)

    case (found: DateTime, expected: Long) => found.getMillis.compareTo(expected)
    case (found: DateTime, expected: DateTime) => found.compareTo(expected)
    case (found: DateTime, expected: Date) => found.toDate.compareTo(expected)

    case (found: Date, expected: Long) => found.getTime.compareTo(expected)
    case (found: Date, expected: Date) => found.compareTo(expected)
    case (found: Date, expected: DateTime) => found.compareTo(expected.toDate)

    case (found: String, expected: Byte) => found.compareTo(expected.toChar.toString)
    case (found: String, expected) => found.compareTo(expected.toString)
    case (found, expected: String) => found.toString.compareTo(expected)

    case (found: Double, expected: Double) => found.compareTo(expected)
    case (found: Double, expected: Float) => found.compareTo(expected.toString.toDouble)
    case (found: Double, expected: Long) => found.compareTo(expected)
    case (found: Double, expected: Int) => found.compareTo(expected)
    case (found: Double, expected: Short) => found.compareTo(expected)
    case (found: Double, expected: Byte) => found.compareTo(expected)

    case (found: Float, expected: Double) if expected.toFloat.toString.toDouble == expected => found.compareTo(expected.toFloat)
    case (found: Float, expected: Float) => found.compareTo(expected)
    case (found: Float, expected: Long) => found.compareTo(expected)
    case (found: Float, expected: Int) => found.compareTo(expected)
    case (found: Float, expected: Short) => found.compareTo(expected)
    case (found: Float, expected: Byte) => found.compareTo(expected)

    case (found: Long, expected: Double) if expected.toLong.toDouble == expected => found.compareTo(expected.toLong)
    case (found: Long, expected: Float) if expected.toLong.toFloat == expected => found.compareTo(expected.toLong)
    case (found: Long, expected: Long) => found.compareTo(expected)
    case (found: Long, expected: Int) => found.compareTo(expected)
    case (found: Long, expected: Short) => found.compareTo(expected)
    case (found: Long, expected: Byte) => found.compareTo(expected)

    case (found: Int, expected: Double) if expected.toInt.toDouble == expected => found.compareTo(expected.toInt)
    case (found: Int, expected: Float) if expected.toInt.toFloat == expected => found.compareTo(expected.toInt)
    case (found: Int, expected: Long) if expected.toInt.toLong == expected => found.compareTo(expected.toInt)
    case (found: Int, expected: Int) => found.compareTo(expected)
    case (found: Int, expected: Short) => found.compareTo(expected)
    case (found: Int, expected: Byte) => found.compareTo(expected)

    case (found: Short, expected: Double) if expected.toShort.toDouble == expected => found.compareTo(expected.toShort)
    case (found: Short, expected: Float) if expected.toShort.toFloat == expected => found.compareTo(expected.toShort)
    case (found: Short, expected: Long) if expected.toShort.toLong == expected => found.compareTo(expected.toShort)
    case (found: Short, expected: Int) if expected.toShort.toInt == expected => found.compareTo(expected.toShort)
    case (found: Short, expected: Short) => found.compareTo(expected)
    case (found: Short, expected: Byte) => found.compareTo(expected)

    case (found: Char, expected: Long) if expected.toString.size == 1 => found.compareTo(expected.toString.head)
    case (found: Char, expected: Int) if expected.toString.size == 1 => found.compareTo(expected.toString.head)
    case (found: Char, expected: Short) if expected.toString.size == 1 => found.compareTo(expected.toString.head)
    case (found: Char, expected: Char) => found.compareTo(expected)
    case (found: Char, expected: Byte) if expected.toChar.toByte == expected => found.compareTo(expected.toChar)

    case (found: Byte, expected: Long) if expected.toByte.toLong == expected => found.compareTo(expected.toByte)
    case (found: Byte, expected: Int) if expected.toByte.toInt == expected => found.compareTo(expected.toByte)
    case (found: Byte, expected: Short) if expected.toByte.toShort == expected => found.compareTo(expected.toByte)
    case (found: Byte, expected: Char) if expected.toByte.toChar == expected => found.compareTo(expected.toByte)
    case (found: Byte, expected: Byte) => found.compareTo(expected)
  }

  protected def noPartialMatch: PartialFunction[(Any, Any), Int] = {
    case (found, expected) =>
      throw new java.lang.RuntimeException("no comparitor found for: " + found + " compareTo " + expected)
  }

  protected def eqMatcher =
  // for practical reasons if a.b.c=10 is what we are provided
  // and a.b.c = [5,10,15] then treat $eq as $in
    (found: Any, expected: Any) =>
      (found, expected) match {
        case (foundSeq: Seq[_], expSeq: Seq[_]) => {
          !expSeq.exists(inMatcher(found, _) == false)
        }
        case (foundSeq: Seq[_], _) => {
          inMatcher(foundSeq, expected)
        }
        case _ => cmpPartialMatcher.lift(found, expected) == Some(0)
      }

  protected def cmpMatcher =
    (found: Any, expected: Any) => {
      val matcher = cmpPartialMatcher orElse noPartialMatch
      matcher(found, expected)
    }

  protected def existsMatcher =
    (found: Any, expected: Any) =>
      found match {
        case None | Nil => expected == false
        case _ => expected == true
      }

  protected def inMatcher =
    (found: Any, expected: Any) => {
      (found, expected) match {
        case (foundSeq: Seq[_], _) => {
          foundSeq.exists(cmpPartialMatcher.lift(_, expected) == Some(0))
        }
        case (_, expSeq: Seq[_]) => {
          expSeq.exists(cmpPartialMatcher.lift(found, _) == Some(0))
        }
      }
    }

  protected def regexMatcher =
    (found: Any, expected: Any) =>
      (expected.toString.r findFirstIn found.toString) != None

  /** dispatch the match operator to the correct matching routine. */
  protected def matchOp(key: String, value: Any, op: String, record: Map[String, Any]): Boolean = {
    // $eq $ne $gt $lt $gte $lte $exists $in $nin $regex
    val opMatcher: (Any, Any) => Boolean = op match {
      case "$eq" => eqMatcher(_, _)
      case "$ne" => !eqMatcher(_, _)
      case "$gt" => cmpMatcher(_, _) > 0
      case "$gte" => cmpMatcher(_, _) >= 0
      case "$lt" => cmpMatcher(_, _) < 0
      case "$lte" => cmpMatcher(_, _) <= 0
      case "$exists" => existsMatcher(_, _)
      case "$in" => inMatcher(_, _)
      case "$nin" => !inMatcher(_, _)
      case "$regex" => regexMatcher(_, _)
      case unk => throw new java.lang.RuntimeException("uknown match operation: " + unk)
    }

    runMatcher(key, value, opMatcher, record)
  }

  /** match the query key to the query value give a matcher routine.  Object
    * heirarchy is indicated via a "." seperator, so "foo.bar" as key woudl indicate
    * there is a "foo" object with a "bar" sub object.  If "foo" is an array
    * we will look for a "bar" in any of the sub objects.
    *
    * @param key they location/name of the data in the record
    * @param value the expected value to be matched against
    * @param opMatcher the matching routine
    * @param record the record to be matched
    * @return true is returned if the data is located and matched
    */
  protected def runMatcher(key: String, value: Any, opMatcher: (Any, Any) => Boolean, record: Map[String, Any]): Boolean = {
    def _findObj(parts: Array[String], value: Any, data: Map[String, Any]): Boolean = {
      val dataHead = data.get(parts.head) match {
        case Some(v) => v
        case _ => None
      }

      if (parts.size == 1) {
        return opMatcher(dataHead, value)
      }
      dataHead match {
        case v: Map[_, _] => _findObj(parts.tail, value, v.asInstanceOf[Map[String, Any]])
        case v: Seq[_] => _findList(parts.tail, value, v.asInstanceOf[Seq[Any]])
        case v => false
      }
    }

    def _findList(parts: Array[String], value: Any, data: Seq[Any]): Boolean = {
      // return true if any item in the list matches the value
      data.exists(v => v match {
        case x: Map[_, _] => _findObj(parts, value, x.asInstanceOf[Map[String, Any]])
        case x: Seq[_] => _findList(parts, value, x.asInstanceOf[Seq[Any]])
        case _ => false
      })
    }
    _findObj(key.split('.'), value, record)
  }
}
