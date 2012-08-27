package com.netflix.edda

import org.slf4j.{Logger, LoggerFactory}

object CollectionManager {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    var collections: Map[String,Queryable] = Map()

    def register(name: String, collection: Queryable) {
        logger.info("Registering collection " + collection)
        collections = collections + (name -> collection)
    }
    
    def get(name: String): Option[Queryable] = {
        collections.get(name)
    }

    def names(): Set[String] = collections.keySet
    
    def start() {
        logger.info("Staring collections")
        collections.values.foreach( _.start() )
    }

    def stop() {
        logger.info("Stopping collections")
        collections.values.foreach( _.stop() )
    }
}
