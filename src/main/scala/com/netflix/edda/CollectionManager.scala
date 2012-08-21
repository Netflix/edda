package com.netflix.edda

import org.slf4j.{Logger, LoggerFactory}

object CollectionManager {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    var collections: Map[String,Collection] = Map()

    def register(name: String, collection: Collection) {
        logger.info("Registering collection " + collection)
        collections = collections + (name -> collection)
    }
    
    def get(name: String): Option[Collection] = {
        collections.get(name)
    }
    
    def start() {
        logger.info("Staring collections")
        collections.values.foreach( _.start() )
    }

    def stop() {
        logger.info("Stopping collections")
        collections.values.foreach( _.stop() )
    }
}
