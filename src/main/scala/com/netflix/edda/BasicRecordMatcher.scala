package com.netflix.edda

trait Matcher {
    def doesMatch (queryMap: Map[String,Any], record: Map[String,Any]): Boolean
}
    
object BasicRecordMatcher extends Matcher {

    def doesMatch (queryMap: Map[String,Any], record: Map[String,Any]): Boolean = {
        // find the first rule where rule does not match record
        // if a rule is found then the query does *not* match and false
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
    def matchOp(key: String, value: Any, op: String, record: Map[String,Any]): Boolean = {
        // $ne $gt $lt $gte $lte $exists $in $nin $nor $or $and $regex
        val opMatcher: (Any,Any) => Boolean = op match {
            case "$eq"     => 
                // for practical reasons if a.b.c=10 is what we are provided
                // and a.b.c = [5,10,15] then treat $eq as $in
                (expected: Any, found: Any) =>
                    found match {
                        case seq: Seq[_] => seq.find( item => expected == item ) match {
                            case Some(_) => true
                            case _ => false
                        }
                        case value => expected == value
                    }
            case "$ne"     => _ != _
            case "$gt"     =>
                (expected: Any, found: Any) =>
                    if( expected.toString.contains('.') ) {
                        BigDecimal(expected.toString) > BigDecimal(found.toString)
                    }
                    else
                        BigInt(expected.toString) > BigInt(found.toString)
            case "$gte"    =>
                (expected: Any, found: Any) =>
                    if( expected.toString.contains('.') ) {
                        BigDecimal(expected.toString) >= BigDecimal(found.toString)
                    }
                    else
                        BigInt(expected.toString) >= BigInt(found.toString)
            case "$lt"     =>
                (expected: Any, found: Any) =>
                    if( expected.toString.contains('.') ) {
                        BigDecimal(expected.toString) < BigDecimal(found.toString)
                    }
                    else
                        BigInt(expected.toString) < BigInt(found.toString)
            case "$lte"    => 
                (expected: Any, found: Any) =>
                    if( expected.toString.contains('.') ) {
                        BigDecimal(expected.toString) <= BigDecimal(found.toString)
                    }
                    else
                        BigInt(expected.toString) <= BigInt(found.toString)
            case "$exists" => 
                (expected: Any, found: Any) => 
                    found match {
                        case null | None | Nil => expected == false
                        case _ => expected == true
                    }
            case "$in"     =>
                (expected: Any, found: Any) =>
                    found match {
                        case seq: Seq[_] => seq.find( item => expected == item ) match {
                            case Some(_) => true
                            case _ => false
                        }
                        case unk => throw new java.lang.RuntimeException("attempted to use $in operator on non-sequence value: " + found)
                    }
            case "$nin"    => 
                (expected: Any, found: Any) =>
                    found.asInstanceOf[List[Any]].find(item => expected == item) match {
                        case Some(_) => false
                        case _ => true
                    }
            case "$regex"  => 
                (expected: Any, found: Any) =>
                    expected.toString.r findFirstIn found.toString match {
                        case Some(_) => true
                        case _ => false
                    }
            case op => throw new java.lang.RuntimeException("uknown match operation: " + op)
        }
            
        runMatcher(key,value,opMatcher,record)
    }
    
    def runMatcher(key: String, value: Any, opMatcher: (Any,Any)=>Boolean, record: Map[String,Any]): Boolean = {
        def _findObj(parts: Array[String], value: Any, data: Map[String,Any]): Boolean = {
            val dataHead = data.get(parts.head) match {
                case Some(value) => value
                case _ => null
            }

            if( parts.size == 1 ) {
                return opMatcher(value, dataHead)
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

