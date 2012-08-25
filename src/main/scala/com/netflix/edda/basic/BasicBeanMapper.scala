package com.netflix.edda.basic

import com.netflix.edda.BeanMapper
import com.netflix.edda.ConfigContext

import java.util.Date

import org.joda.time.DateTime

import org.slf4j.{Logger, LoggerFactory}

import org.apache.commons.beanutils.BeanMap

class BasicBeanMapper(ctx: ConfigContext) extends BeanMapper {
    private[this] val logger = LoggerFactory.getLogger(getClass)

    def apply(obj: Any): Any = {
        mkValue(obj).getOrElse(null)
    }

//    val ignorePattern = ctx.config.getProperty("edda.bean.ignorePattern", "(?i)(timestamp|clock)$").r
    val argPattern = ctx.config.getProperty("edda.bean.argPattern", "[^a-zA-Z0-9_]").r

    /** Create a mongo db list from a java collection object. */
    def mkList(c: java.util.Collection[_ <: Any]): List[Any] = {
        import collection.JavaConverters._
        c.asScala
         .map(v => mkValue(v).getOrElse(null))
         .toList
         .sortBy(v => if(v == null) "" else v.toString.toLowerCase)
    }

    /** Create a mongo db object from a java map object. */
    def mkMap(m: java.util.Map[_ <: Any, _ <: Any]): Map[Any, Any] = {
        import scala.collection.JavaConverters._
        if (m.getClass.isEnum)
            Map(
                "class" -> m.getClass.getName,
                "name" -> m.getClass.getMethod("name").invoke(m).asInstanceOf[String]
            )
        else
            m.asScala.collect({
//                case (key: Any, value: Any) if ignorePattern.findFirstIn(key.toString) == None =>
                case (key: Any, value: Any) =>
                    argPattern.replaceAllIn(key.toString,"_") -> mkValue(value).getOrElse(null)
            }).toMap[Any,Any] + ("class" -> m.getClass.getName)
    }

    def mkValue(value: Any): Option[Any] = value match {
        case v: Boolean  => Some(v)
        case v: Byte     => Some(v)
        case v: Int      => Some(v)
        case v: Short    => Some(v)
        case v: Long     => Some(v)
        case v: Float    => Some(v)
        case v: Double   => Some(v)
        case v: Char     => Some(v)
        case v: String   => Some(v) 
        case v: Date     => Some(new DateTime(v)) 
        case v: DateTime => Some(v) 
        case v: Class[_] => Some(v.getName)
        case v: java.util.Collection[_] => Some(mkList(v))
        case v: java.util.Map[_,_] => Some(mkMap(v))
        case v: AnyRef   => Some(fromBean(v))
        case null => Some(null)
        case other       => {
            logger.warn("dont know how to make value from " + other)
            None
        }
    }

    private[this] var keyMappers: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
        case (obj,key,value) => value
    }
    
    def fromBean(obj: AnyRef): AnyRef = {
        import scala.collection.JavaConverters._
        if (obj.getClass.isEnum) {
            Map(
                "class" -> obj.getClass.getName,
                "name" -> obj.getClass.getMethod("name").invoke(obj).asInstanceOf[String]
            )
        } else {
            val beanMap = new BeanMap(obj)
            val entries = beanMap.entrySet.asScala.toList.sortBy(_.asInstanceOf[java.util.Map.Entry[String,Any]].getKey.toLowerCase)
            entries.map(
                item => {
                    val entry = item.asInstanceOf[java.util.Map.Entry[String,Any]]
                    val value = mkValue(entry.getValue)
                    entry.getKey -> keyMappers(obj,entry.getKey,value)
                }
            ).collect({
//                case (name: String, Some(value)) if ignorePattern.findFirstIn(name) == None =>
                case (name: String, Some(value)) =>
                    argPattern.replaceAllIn(name,"_") -> value
            }).toMap
        }
    }

    def addKeyMapper( pf: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] ) {
        keyMappers = pf orElse keyMappers
    }
}
