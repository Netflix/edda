package com.netflix.edda

trait RecordMatcher {
  def doesMatch(queryMap: Map[String, Any], record: Map[String, Any]): Boolean
}

