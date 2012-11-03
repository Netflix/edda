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

object Utils {
  private lazy val factory = new MappingJsonFactory
  private lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")

  case class NamedActor[T](name: String)(body: => T) extends DaemonActor {
    override def toString = name

    override def act() {
      body
    }

    var handlers: PartialFunction[Exception,Unit] = Map()
    def addExceptionHandler(pf: PartialFunction[Exception,Unit]) {
        handlers = pf orElse handlers 
    }

    override def exceptionHandler = handlers

    start()
  }

  // allow for hierarchical properties
  // so if we have prefix = "p", propName = "n", nameContext  = "a.b.c" then
  // it will look for p.a.b.c.n, then p.a.b.n, then p.a.n, then p.n else return default
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

  def toJson(obj: Any): String = {
    val baos = new ByteArrayOutputStream()
    val gen = factory.createJsonGenerator(baos, UTF8)
    writeJson(gen, obj)
    gen.close()
    baos.toString
  }

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

  def dateFormatter(arg: Any): Any = {
    arg match {
      case v: Date => dateFormat.format(v)
      case v: DateTime => v.toString("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
      case v => v
    }
  }

  // most recent records first
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
