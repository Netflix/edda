package com.netflix.edda

object CollectionManager {
    var collections: Map[String,Collection] = Map()

    def register(name: String, collection: Collection) {
        collections = collections + (name -> collection)
    }
    
    def get(name: String): Option[Collection] = {
        collections.get(name)
    }
}
