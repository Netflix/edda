



package com.netflix.edda.basic

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHandler
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory

object MinimalServletInScala {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]) {
    val server = new Server(8080)
    val handler = new ServletHandler()
    server.setHandler(handler)
    handler.addServletWithMapping(classOf[BasicServer], "/*")
    server.start()
    server.join()
  }
  
}