/*
 * Copyright 2012-2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda.basic

import com.netflix.edda.BeanMapper
import com.netflix.edda.Utils

import java.util.Date

import org.joda.time.DateTime

import org.slf4j.LoggerFactory

import org.apache.commons.beanutils.BeanMap



/** Class to convert a Java Bean to a primitive Scala object (Map, Seq, etc)
 */
class BasicBeanMapper extends BeanMapper {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  // private[this] val stringInterner = new com.google.common.collect.Interner(com.google.common.collect.Interners.newWeakInterner[String])

  def apply(obj: Any): Any = {
    mkValue(obj).getOrElse(null)
  }

  val argPattern = Utils.getProperty("edda", "bean.argPattern", "", "[^a-zA-Z0-9_]").get.r

  /** Create a Mongodb list from a java collection object. */
  def mkList(c: java.util.Collection[_ <: Any]): List[Any] = {
    import collection.JavaConverters._
    c.asScala
      .map(v => mkValue(v).getOrElse(null))
      .toList
      .sortBy(v => if (v == null) "" else v.toString.toLowerCase)
  }

  /** Create a Mongodb object from a java map object. */
  def mkMap(m: java.util.Map[_ <: Any, _ <: Any]): Map[Any, Any] = {
    import scala.collection.JavaConverters._
    if (m.getClass.isEnum)
      Map(
        "class" -> m.getClass.getName,
        "name" -> m.getClass.getMethod("name").invoke(m).asInstanceOf[String])
    else
      m.asScala.collect({
        case (key: Any, value: Any) =>
          argPattern.replaceAllIn(key.toString, "_").intern -> mkValue(value).getOrElse(null)
      }).toMap[Any, Any] + ("class" -> m.getClass.getName)
  }

  /** Create Any value into a Option of corresponding Scala value */
  def mkValue(value: Any): Option[Any] = value match {
    case v: Boolean => Some(v)
    case v: Byte => Some(v)
    case v: Int => Some(v)
    case v: Short => Some(v)
    case v: Long => Some(v)
    case v: Float => Some(v)
    case v: Double => Some(v)
    case v: Char => Some(v)
    case v: String => Some(v)
    case v: Date => Some(new DateTime(v))
    case v: DateTime => Some(v)
    case v: Class[_] => Some(v.getName)
    case v: java.util.Collection[_] => Some(mkList(v))
    case v: java.util.Map[_, _] => Some(mkMap(v))
    case v: AnyRef => Some(fromBean(v))
    case null => Some(null)
    case other => {
      if (logger.isWarnEnabled) logger.warn("dont know how to make value from " + other)
      None
    }
  }

  private[this] var objMappers: PartialFunction[AnyRef, AnyRef] = {
    case obj => {
      import scala.collection.JavaConverters._
      val beanMap = new BeanMap(obj)
      val entries = beanMap.entrySet.asScala.toList.sortBy(_.asInstanceOf[java.util.Map.Entry[String, Any]].getKey.toLowerCase)
      entries.map(
        item => {
          val entry = item.asInstanceOf[java.util.Map.Entry[String, Any]]
          val value = mkValue(entry.getValue)
          entry.getKey -> keyMappers(obj, entry.getKey, value)
        }).collect({
        case (name: String, Some(value)) =>
          argPattern.replaceAllIn(name, "_").intern -> value
      }).toMap
    }
  }

  private[this] var keyMappers: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
    case (obj, key, value) => value
  }

  /** Creates scala Map from Java Bean, adding "class" map attribute to what class the Bean
    * was originally */
  def fromBean(obj: AnyRef): AnyRef = {
    if (obj.getClass.isEnum) {
      Map(
        "class" -> obj.getClass.getName,
        "name" -> obj.getClass.getMethod("name").invoke(obj).asInstanceOf[String])
    } else {
      objMappers(obj)
    }
  }

  /** to specialize a Bean transformation you can create a custom Object Mapper
    *
    * {{{
    * val ignoreObjMapper: PartialFunction[AnyRef,AnyRef] = {
    *    case obj if obj.getClass.getName == "com.netflix.useless.details" => null
    * }
    * beanMapper.addObjMapper(ignoreObjMapper)
    * }}}
    *
    * @param pf partial function which takes an object and returns an object
    */
  def addObjMapper(pf: PartialFunction[AnyRef, AnyRef]) {
    objMappers = pf orElse objMappers
  }


  /** to specialize a Bean transformation you can create a custom Object Mapper which
    * translates specific attributes to whatever is needed
    *
    * A common case might be to ignore timestamp attributes from a bean
    *
    * {{{
    * val timestampKeyMapper: PartialFunction[(AnyRef,String,Option[Any]),Option[Any]] = {
    *    case (obj, "ModifiedTime", value) => Some(0)
    *}
    * beanMapper.addKeyMapper(timestampKeyMapper)
    * }}}
    *
    * @param pf partial function which takes a tuble of (Object, String (attribute name), Current Attribute value) and returns
    *           an optional value (or None if you want the attribute deleted)
    */
  def addKeyMapper(pf: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]]) {
    keyMappers = pf orElse keyMappers
  }
}
