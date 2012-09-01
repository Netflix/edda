package com.netflix.edda

import scala.actors.Actor

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.Date
import java.text.SimpleDateFormat

import org.joda.time.DateTime

import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.JsonEncoding.UTF8
import org.codehaus.jackson.util.DefaultPrettyPrinter
import org.codehaus.jackson.map.MappingJsonFactory

import org.slf4j.{Logger, LoggerFactory}

object Utils {
    private[this] val logger = LoggerFactory.getLogger(getClass)
    private lazy val factory = new MappingJsonFactory
    private lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
    private lazy val dpp = {
        val dpp = new DefaultPrettyPrinter
        dpp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
        dpp
    }

    case class NamedActor[T](name: String)(body: => T) extends Actor {
        override def toString = name
        override def act = body
        start
    }

    def toObjects(args: Any*): Array[AnyRef] = {
        args.map(arg => arg match {
            case v: Char    => v.asInstanceOf[java.lang.Character]
            case v: Byte    => v.asInstanceOf[java.lang.Byte]
            case v: Short   => v.asInstanceOf[java.lang.Short]
            case v: Int     => v.asInstanceOf[java.lang.Integer]
            case v: Long    => v.asInstanceOf[java.lang.Long]
            case v: Float   => v.asInstanceOf[java.lang.Float]
            case v: Double  => v.asInstanceOf[java.lang.Double]
            case v: Boolean => v.asInstanceOf[java.lang.Boolean]
            case (v: Any, f: String) => f.format(v)
            case v: AnyRef  => v
        }).toArray[AnyRef]
    }

    def toPrettyJson(obj: Any): String = {
        val baos = new ByteArrayOutputStream()
        val gen = factory.createJsonGenerator(baos, UTF8)
        gen.setPrettyPrinter(dpp)
        toJson(gen, obj, dateFormatter)
        gen.close;
        baos.toString
    }

    def toJson(obj: Any): String = {
        val baos = new ByteArrayOutputStream()
        val gen = factory.createJsonGenerator(baos, UTF8)
        toJson(gen, obj)
        gen.close;
        baos.toString
    }

    def toJson(gen: JsonGenerator, obj: Any, fmt: (Any) => Any = (x: Any) => x) {
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
            case v: Map[_,_] => {
                gen.writeStartObject
                v.toSeq.sortBy(_._1.asInstanceOf[String]).foreach( pair => {
                    gen.writeFieldName(pair._1.toString)
                    toJson(gen, pair._2, fmt)
                })
                gen.writeEndObject
            }
            case v: Seq[_] => {
                gen.writeStartArray
                v.foreach( toJson(gen, _, fmt) )
                gen.writeEndArray
            }
            case null => gen.writeNull
            case v => { 
                throw new java.lang.RuntimeException("unable to convert \"" + v + "\" to json")
            }
        }
    }

    def dateFormatter(arg: Any): Any = {
        arg match {
            case v: Date => dateFormat.format(v)
            case v: DateTime => dateFormat.format(v.toDate)
            case v => v
        }
    }

    // most recent records first
    def diffRecords(recs: Seq[Record], context: Option[Int] = None, prefix: String  = ""): String = {
        import difflib.DiffUtils;
        import difflib.Patch;
        import scala.collection.JavaConverters._
        if( recs.size < 2 ) {
            throw new java.lang.RuntimeException("diff requires at least 2 records")
        }
        // map each record to a tuple of it's id uri and pretty-printed string output
        // then use 2-wide sliding window and create unified diffs for each pair
        val result = new collection.mutable.StringBuilder
        recs.map(rec => {
            if( rec == null ) {
                ("/dev/null", "")
            } else {
                val baos = new ByteArrayOutputStream()
                val gen = factory.createJsonGenerator(baos, UTF8)
                dpp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
                gen.setPrettyPrinter(dpp)
                toJson(gen, rec.data, dateFormatter)
                gen.close;
                (prefix + "/" + rec.id + ";_pp;_at=" + rec.stime.getMillis, baos.toString)
            }
        }).sliding(2).foreach( v => {
            var(a,b) = (v.head,v.tail.head)
            val alines = a._2.split("\n").toList
            val blines = b._2.split("\n").toList
            val size =
                if ( context != None  ) context.get
                else if( alines.length > blines.length ) alines.length else blines.length
            val patch: Patch = DiffUtils.diff(blines.asJava,alines.asJava)
            DiffUtils.generateUnifiedDiff(b._1,a._1,blines.asJava,patch,size).asScala.foreach(l => {
                result.append(l)
                result.append('\n')
            });
        })
        result.toString
    }
}
