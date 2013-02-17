/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda

import scala.actors.DaemonActor

import java.io.ByteArrayOutputStream
import java.util.Date
import java.util.Properties
import java.text.SimpleDateFormat

import org.joda.time.DateTime

import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.JsonEncoding.UTF8
import org.codehaus.jackson.util.DefaultPrettyPrinter
import org.codehaus.jackson.map.MappingJsonFactory
import org.codehaus.jackson.JsonNode

/** singleton object for various helper functions */
object Utils {
  private lazy val factory = new MappingJsonFactory
  private lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")

  class EventStatus() {}
  
  type EventHandlers = PartialFunction[EventStatus,Unit]
  
  case class Success(event: Any) extends EventStatus
  case class Failure(event: Any) extends EventStatus
  
  val DefaultEventHandlers: EventHandlers = {
    case Success(event) => 
    case Failure(event) => throw new java.lang.RuntimeException(event.toString)
  }

  /** class used to assist logging and allow for abstracted exception handling
    * for simple actors
    * @param name name of actor that is seen when logging
    * @param body closure run as the actors "act" routine
    */
  case class NamedActor(name: String)(body: => Unit) extends DaemonActor {
    override def toString = name

    override def act() {
      body
    }

    var handlers: PartialFunction[Exception,Unit] = Map()

    /** add a partial function to allow for specific exception
      * handling when needed
      * @param pf PartialFunction to handle exception types
      */
    def addExceptionHandler(pf: PartialFunction[Exception,Unit]) {
        handlers = pf orElse handlers 
    }

    /** setup exceptionHandler to use the custom handlers modified
      * with addExceptionHandler
      */
    override def exceptionHandler = handlers

    start()
  }

  /** allow for hierarchical properties
    * {{{
    * if we have prefix = "edda.collection",
    *            propName = "enabled",
    *            nameContext  = "test.us-east-1.aws.addresses"
    * then it will look for (in this order):
    *     edda.collection.test.us-east-1.aws.addresses.enabled
    *     edda.collection.test.us-east-1.aws.enabled
    *     edda.collection.test.us-east-1.enabled
    *     edda.collection.test.enabled
    *     edda.collection.enabled
    * else return default
    * }}}
    * @param props group of available properties
    * @param prefix root prefix, generally "edda.something"
    * @param propName property name (ie "enabled")
    * @param nameContext set property names to search though
    * @param default the default value to return if no matching properties are found
    * @return the best matching property value
    */
  def getProperty(props: Properties, prefix: String, propName: String, nameContext: String, default: String): String = {
    val parts = nameContext.split('.')
    Range(1, parts.size + 1).reverse.map(
      ix => prefix + "." + parts.take(ix).mkString(".") + "." + propName
    ) collectFirst {
      case prop: String if props.containsKey(prop) => props.getProperty(prop)
    } match {
      case Some(v) => v
      case None => props.getProperty(prefix + "." + propName, default)
    }
  }

  /** convert list of Any to list of AnyRef.  This is useful for slf4j printf style formatting:
    * {{{
    * logger.info("stuff {} {} {} {}", toObjects(1, 1.2, true, "string"))
    * }}}
    * @param args list of items to massage into list of AnyRef
    */
  def toObjects(args: Any*): Array[AnyRef] = {
    args.map(arg => arg match {
      case null => null
      case v: Char => v.asInstanceOf[java.lang.Character]
      case v: Byte => v.asInstanceOf[java.lang.Byte]
      case v: Short => v.asInstanceOf[java.lang.Short]
      case v: Int => v.asInstanceOf[java.lang.Integer]
      case v: Long => v.asInstanceOf[java.lang.Long]
      case v: Float => v.asInstanceOf[java.lang.Float]
      case v: Double => v.asInstanceOf[java.lang.Double]
      case v: Boolean => v.asInstanceOf[java.lang.Boolean]
      case (v: Any, f: String) => f.format(v)
      case v: AnyRef => v
    }).toArray[AnyRef]
  }

  /** convert an object to a json string, using the pretty printer formatter */
  def toPrettyJson(obj: Any): String = {
    val baos = new ByteArrayOutputStream()
    val gen = factory.createJsonGenerator(baos, UTF8)
    val dpp = new DefaultPrettyPrinter
    dpp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
    gen.setPrettyPrinter(dpp)
    writeJson(gen, obj, dateFormatter)
    gen.close()
    baos.toString
  }

  /** convert an object to a json string */
  def toJson(obj: Any): String = {
    val baos = new ByteArrayOutputStream()
    val gen = factory.createJsonGenerator(baos, UTF8)
    writeJson(gen, obj)
    gen.close()
    baos.toString
  }

  /** given an JsonGenerator, write object to generator.  Apply
    * formatter to possible translate data (ie convert millisecond
    * timetamps to humanreadable time strings)
    * @param gen JsonGenerator to write to
    * @param obj object to convert to json
    * @param fmt abitrary object translator
    */
  def writeJson(gen: JsonGenerator, obj: Any, fmt: (Any) => Any = (x: Any) => x) {
    fmt(obj) match {
      case v: Boolean => gen.writeBoolean(v)
      case v: Byte => gen.writeNumber(v)
      case v: Short => gen.writeNumber(v)
      case v: Int => gen.writeNumber(v)
      case v: Long => gen.writeNumber(v)
      case v: Float => gen.writeNumber(v)
      case v: Double => gen.writeNumber(v)
      case v: Char => gen.writeString("" + v)
      case v: String => gen.writeString(v)
      case v: Date => gen.writeNumber(v.getTime)
      case v: DateTime => gen.writeNumber(v.getMillis)
      case v: Map[_, _] => {
        gen.writeStartObject()
        v.toSeq.sortBy(_._1.asInstanceOf[String]).foreach(pair => {
          gen.writeFieldName(pair._1.toString)
          writeJson(gen, pair._2, fmt)
        })
        gen.writeEndObject()
      }
      case v: Seq[_] => {
        gen.writeStartArray()
        v.foreach(writeJson(gen, _, fmt))
        gen.writeEndArray()
      }
      case null => gen.writeNull()
      case v => {
        throw new java.lang.RuntimeException("unable to convert \"" + v + "\" [" + v.getClass + "] to json")
      }
    }
  }

  /** convert a JsonNode to a scala value */
  def fromJson(node: JsonNode): Any = {
    import scala.collection.JavaConverters._
    node match {
      case _ if node.isBigDecimal => node.getDecimalValue
      case _ if node.isBigInteger => node.getBigIntegerValue
      case _ if node.isBoolean => node.getBooleanValue
      case _ if node.isLong => node.getLongValue
      case _ if node.isInt => node.getIntValue
      case _ if node.isDouble => node.getDoubleValue
      case _ if node.isTextual => node.getTextValue
      case _ if node.isNull => null
      case _ if node.isObject => {
        node.getFieldNames.asScala.map(
          key => key -> fromJson(node.get(key))
        ).toMap
      }
      case _ if node.isArray => node.getElements.asScala.map(fromJson(_)).toList
      case _ => throw new java.lang.RuntimeException("unable to convert from Json to Scala: " + node)
    }
  }

  /** default date formatter used for json pretty-printing objects with dates in them */
  def dateFormatter(arg: Any): Any = {
    arg match {
      case v: Date => dateFormat.format(v)
      case v: DateTime => v.toString("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
      case v => v
    }
  }

  /** diff multiple records, return unifed diff output
    *
    * @param recs records to diff, pre-sorted, oldest first
    * @param context how much diff context to return, default is entire document
    * @param prefix uri prefix using when writing new/old document names for diff output
    * @return string of unified diff output
    */
  def diffRecords(recs: Seq[Record], context: Option[Int] = None, prefix: String = ""): String = {
    import difflib.DiffUtils
    import difflib.Patch
    import scala.collection.JavaConverters._
    if (recs.size < 2) {
      throw new java.lang.RuntimeException("diff requires at least 2 records")
    }
    // map each record to a tuple of it's id uri and pretty-printed string output
    // then use 2-wide sliding window and create unified diffs for each pair
    val result = new collection.mutable.StringBuilder
    recs.map(rec => {
      if (rec == null) {
        ("/dev/null", "")
      } else {
        val baos = new ByteArrayOutputStream()
        val gen = factory.createJsonGenerator(baos, UTF8)
        val dpp = new DefaultPrettyPrinter
        dpp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
        gen.setPrettyPrinter(dpp)
        writeJson(gen, rec.data, dateFormatter)
        gen.close()
        (prefix + "/" + rec.id + ";_pp;_at=" + rec.stime.getMillis, baos.toString)
      }
    }).sliding(2).foreach(v => {
      val (a, b) = (v.head, v.tail.head)
      val aLines = a._2.split("\n").toList
      val bLines = b._2.split("\n").toList
      val size =
        if (context != None) context.get
        else if (aLines.length > bLines.length) aLines.length else bLines.length
      val patch: Patch = DiffUtils.diff(bLines.asJava, aLines.asJava)
      DiffUtils.generateUnifiedDiff(b._1, a._1, bLines.asJava, patch, size).asScala.foreach(l => {
        result.append(l)
        result.append('\n')
      })
    })
    result.toString()
  }
}
