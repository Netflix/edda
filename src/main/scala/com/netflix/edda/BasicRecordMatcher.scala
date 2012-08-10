package com.netflix.edda

import java.util.Date
import org.joda.time.DateTime

trait Matcher {
    def doesMatch (queryMap: Map[String,Any], record: Map[String,Any]): Boolean
}
    
object BasicRecordMatcher extends Matcher {

    def doesMatch (queryMap: Map[String,Any], record: Map[String,Any]): Boolean = {
        // find the first rule where rule does not match record
        // if a rule is expected then the query does *not* match and false
        // is returned
        queryMap.find(
            rule => ! matchRule(rule, record)
        ) match {
            case Some(_) => false
            case _ => true
        }
    }

    protected
    def matchRule(rule: Any, record: Map[String,Any]): Boolean = {
        rule match {
            // { key: { $op1: val, $op2: val } } ==>
            // { key: { $op1: val }, key: { $op2: val } {
            case (key: String, value: Map[_,_]) =>
                value.find(
                    subrule => ! matchRule((key,subrule), record)
                ) match {
                    case Some(_) => false
                    case _ => true
                }
            // { $or: [ {key: value}, {key: value} ] }
            case ("$or",  value: Seq[_]) => 
                value.find(
                    subrule => matchRule(subrule, record)
                ) match {
                    case Some(_) => true
                    case _ => false
                }
            // { $and: [ {key: value}, {key: value} ] }
            case ("$and", value: Seq[_]) => 
                value.find(
                    subrule => !matchRule(subrule, record)
                ) match {
                    case Some(_) => false
                    case _ => true
                }
            case (key: String, (op: String, value: Any)) => matchOp(key,value,op,record)
            case (key: String, value: Any)               => matchOp(key,value,"$eq",record)
        }
    }

    protected
    def cmpPartialMatcher: PartialFunction[(Any,Any), Int] = {
        case (null, null)     => 0
        case (found, null)    => -1
        case (null, expected) => 1

        case (found: Boolean, expected: String) => found.compareTo(expected.toBoolean)
        case (found: String,  expected: Boolean) => found.toBoolean.compareTo(expected)
        case (found: Boolean, expected: Boolean) => found.compareTo(expected)

        case (found: DateTime, expected: Long) => found.getMillis.compareTo(expected)
        case (found: DateTime, expected: DateTime) => found.compareTo(expected)

        case (found: Date, expected: Long) => found.getTime.compareTo(expected)
        case (found: Date, expected: Date) => found.compareTo(expected)

        case (found: String, expected) => found.compareTo(expected.asInstanceOf[String])
        case (found, expected: String) => found.asInstanceOf[String].compareTo(expected)

        case (found, expected: Double) => found.asInstanceOf[Double].compareTo(expected)
        case (found: Double, expected) => found.compareTo(expected.asInstanceOf[Double])

        case (found, expected: Float) => found.asInstanceOf[Float].compareTo(expected)
        case (found: Float, expected) => found.compareTo(expected.asInstanceOf[Float])

        case (found, expected: Long) => found.asInstanceOf[Long].compareTo(expected)
        case (found: Long, expected) => found.compareTo(expected.asInstanceOf[Long])

        case (found, expected: Int) => found.asInstanceOf[Int].compareTo(expected)
        case (found: Int, expected) => found.compareTo(expected.asInstanceOf[Int])

        case (found, expected: Short) => found.asInstanceOf[Short].compareTo(expected)
        case (found: Short, expected) => found.compareTo(expected.asInstanceOf[Short])

        case (found, expected: Char) => found.asInstanceOf[Char].compareTo(expected)
        case (found: Char, expected) => found.compareTo(expected.asInstanceOf[Char])

        case (found, expected: Byte) => found.asInstanceOf[Byte].compareTo(expected)
        case (found: Byte, expected) => found.compareTo(expected.asInstanceOf[Byte])
    }
    
    protected
    def noPartialMatch: PartialFunction[(Any,Any), Int] = {
        case( found, expected ) => 
            throw new java.lang.RuntimeException("no comparitor found for: " + found + " compareTo " + expected)
    }

    protected
    def eqMatcher =
        // for practical reasons if a.b.c=10 is what we are provided
        // and a.b.c = [5,10,15] then treat $eq as $in
        (found: Any, expected: Any) =>
            found match {
                case seq: Seq[_] => inMatcher(found,expected)
                case value => cmpPartialMatcher.lift(value,expected) == Some(0)
            }

    protected
    def cmpMatcher = 
        (found: Any, expected: Any) => {
            val matcher = cmpPartialMatcher orElse noPartialMatch
            matcher(found, expected)
        }

    protected
    def existsMatcher = 
        (found: Any, expected: Any) => 
            found match {
                case null | None | Nil => found == false
                case _ => found == true
            }

    protected
    def inMatcher =
        (found: Any, expected: Any) =>
            found match {
                case seq: Seq[_] => seq.find( item => cmpPartialMatcher.lift(found,item) == Some(0) ) match {
                    case Some(_) => true
                    case _ => false
                }
                case unk => throw new java.lang.RuntimeException("attempted to use $in operator on non-sequence value: " + found)
            }

    protected
    def regexMatcher = 
        (found: Any, expected: Any) => 
            expected.toString.r findFirstIn found.toString match {
                case Some(_) => true
                case _ => false
            }
    
        
    
    protected
    def matchOp(key: String, value: Any, op: String, record: Map[String,Any]): Boolean = {
        // $eq $ne $gt $lt $gte $lte $exists $in $nin $regex
        val opMatcher: (Any,Any) => Boolean = op match {
            case "$eq"     => eqMatcher(_,_)
            case "$ne"     => !eqMatcher(_,_)
            case "$gt"     => cmpMatcher(_,_) > 0
            case "$gte"    => cmpMatcher(_,_) >= 0
            case "$lt"     => cmpMatcher(_,_) < 0
            case "$lte"    => cmpMatcher(_,_) <= 0
            case "$exists" => existsMatcher(_,_)
            case "$in"     => inMatcher(_,_)
            case "$nin"    => !inMatcher(_,_)
            case "$regex"  => regexMatcher(_,_)
            case op => throw new java.lang.RuntimeException("uknown match operation: " + op)
        }
            
        runMatcher(key,value,opMatcher,record)
    }
    
    protected
    def runMatcher(key: String, value: Any, opMatcher: (Any,Any)=>Boolean, record: Map[String,Any]): Boolean = {
        def _findObj(parts: Array[String], value: Any, data: Map[String,Any]): Boolean = {
            val dataHead = data.get(parts.head) match {
                case Some(value) => value
                case _ => null
            }

            if( parts.size == 1 ) {
                return opMatcher(dataHead,value)
            }
            dataHead match {
                case v: Map[_,_]      => _findObj(parts.tail,  value, v.asInstanceOf[Map[String,Any]])
                case v: Seq[_]        => _findList(parts.tail, value, v.asInstanceOf[Seq[Any]])
                case v => false
            }
        }
        
        def _findList(parts: Array[String], value: Any, data: Seq[Any]): Boolean = {
            // return true if any item in the list matches the value
            data.find( v => v match {
                case v: Map[_,_]      => _findObj(parts,  value, v.asInstanceOf[Map[String,Any]])
                case v: Seq[_]        => _findList(parts, value, v.asInstanceOf[Seq[Any]])
                case v => false
            }) match {
                case Some(_) => true
                case _ => false
            }
        }
        _findObj(key.split('.'),value,record)
    }
}

