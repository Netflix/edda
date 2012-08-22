package com.netflix.edda

import org.codehaus.jackson.JsonGenerator
import java.util.Date
import org.joda.time.DateTime

object Utils {
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
            case v: Seq[_] => {
                gen.writeStartArray
                v.foreach( toJson(gen, _, fmt) )
                gen.writeEndArray
            }
            case v: Map[_,_] => {
                gen.writeStartObject
                v.foreach( pair => {
                    gen.writeFieldName(pair._1.toString)
                    toJson(gen, pair._2, fmt)
                })
                gen.writeEndObject
            }
            case null => gen.writeNull
            case v => { 
                throw new java.lang.RuntimeException("unable to convert \"" + v + "\" to json")
            }
        }
    }
}
