package com.netflix.edda

import java.util.Properties

trait DatastoreComponent {
    val datastore: Option[Datastore]
}

trait Datastore {
    def init()
    def query(queryMap: Map[String,Any], limit: Int): List[Record]
    def load(): List[Record]
    def update(d: Collection.Delta)
}
