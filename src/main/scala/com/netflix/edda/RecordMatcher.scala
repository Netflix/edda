package com.netflix.edda

trait RecordMatcherComponent {
    val recordMatcher: RecordMatcher
}

trait RecordMatcher {
    def doesMatch (queryMap: Map[String,Any], record: Map[String,Any]): Boolean
}
    
