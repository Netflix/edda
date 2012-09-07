package com.netflix.edda

import java.util.Properties

trait Datastore {
    def init()
    def query(queryMap: Map[String,Any], limit: Int, keys: Set[String]): Seq[Record]
    def load(): Seq[Record]
    def update(d: Collection.Delta)
}
