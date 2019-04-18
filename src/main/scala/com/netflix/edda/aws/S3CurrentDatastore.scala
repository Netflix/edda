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
package com.netflix.edda.aws

import com.netflix.edda.Datastore
import com.netflix.edda.RequestId
import com.netflix.edda.Record
import com.netflix.edda.RecordSet
import com.netflix.edda.Collection
import com.netflix.edda.Utils

import org.slf4j.LoggerFactory

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.s3.model._

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.util.Scanner

import org.apache.commons.io.IOUtils
import org.apache.commons.codec.binary.Base64

import org.joda.time.DateTime

case class State(location: String, mtime: DateTime)

class S3CurrentDatastore(val name: String) extends Datastore {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  lazy val account = {
    Utils.getProperty("edda", "s3current.account", name, "").get match {
      case "" => {
        val nameParts = name.split('.')
        if( nameParts.size == 2 ) "" else nameParts.dropRight(2).mkString(".")
      }
      case acct => acct
    }
  }

  lazy val s3     = new AwsClient(account).s3
  val readDynamo = new AwsClient(account).dynamo
  // disable retry's when writing to dynamo ... if initial request
  // gets a timeout we need to know as it will likely complete eventually
  // and then all subsequent conditional updates will fail since will be out
  // of sync with the datastore
  val writeDynamo = {
    val client = new AwsClient(account).dynamo
    client
  }


  lazy val tableName = Utils.getProperty("edda", "s3current.table", name, "edda-s3current-collection-index").get
  lazy val readCap   = Utils.getProperty("edda", "s3current.readCapacity", name, "1").get.toLong
  lazy val writeCap  = Utils.getProperty("edda", "s3current.writeCapacity", name, "1").get.toLong
  lazy val bucketName = Utils.getProperty("edda", "s3current.bucket", name, "edda-s3current-collections").get

  lazy val locationPrefix = Utils.getProperty("edda", "s3current.locationPrefix", name, "edda/s3current").get

  lazy val autoDelete = Utils.getProperty("edda", "s3current.autoDelete", name, "false")
    
  def init() {
    implicit var client = writeDynamo
    s3.getBucketLocation(bucketName) match {
      case "EU"|"eu-west-1" => s3.setEndpoint("s3-eu-west-1.amazonaws.com")
      case "US"|"" => s3.setEndpoint("s3.amazonaws.com")
      case location => s3.setEndpoint(s"s3-$location.amazonaws.com")
    }
    DynamoDB.init(tableName, readCap, writeCap)
  }

  override def load(replicaOk: Boolean)(implicit req: RequestId): RecordSet = {
    // load once, if we get a 404 from S3, then try once more
    try {
      loadImpl(replicaOk)
    } catch {
      // object does not exist, which might mean it was just deleted, so just try to reload
      case e: AmazonS3Exception if e.getStatusCode == 404 => {
        loadImpl(replicaOk)
      }
      // if we get a socket timeout then try once more, probably a temp network glitch
      case e: java.net.SocketTimeoutException => loadImpl(replicaOk)
    }
  }

  def loadImpl(replicaOk: Boolean)(implicit req: RequestId): RecordSet = {
    // load from DynamoDB
    import collection.JavaConverters._
    implicit var client = readDynamo

    val item = DynamoDB.get(tableName, "name", name) match {
      case Some(record: Map[String,String]) => record
      case _ => throw new java.lang.UnsupportedOperationException(s"Dynamo record for $name not found")
    }

    val location  = item("location")
    val requestId = item("reqId")
    logger.info(s"$req$this Loading $name: $location ($requestId)")

    // read from S3
    val t0 = System.nanoTime()
    var md5: String = null
    var mtime: DateTime = DateTime.now
    var userMeta = Map[String,String]()
    val bytes = try {
      val s3Object = s3.getObject(bucketName, location)
      val meta = s3Object.getObjectMetadata();
      mtime = new DateTime(meta.getLastModified())
      
      userMeta = meta.getUserMetadata().asScala.toMap
      val origReqId = userMeta("reqid")
      md5 = userMeta.get("md5").getOrElse("")
      logger.info(s"$req$this Loaded $name: $location [$md5] ($origReqId) modifed: $mtime")
      
      val inputStream = s3Object.getObjectContent
      var out = IOUtils.toByteArray(inputStream)
      inputStream.close()
      out
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req$this s3 read lapse: ${lapse}ms")
    }

    var newMD5 = Base64.encodeBase64String(MessageDigest.getInstance("MD5").digest(bytes)).trim
    var jsonContent = if( userMeta.get("compressed").getOrElse("false").toBoolean ) {
      Utils.decompress(bytes)
    } else {
      IOUtils.toString(bytes, "UTF-8")
    }

    // MD5 check content
    if( ! md5.isEmpty ) {
      if( md5 != newMD5 ) {
        logger.error(s"$req$this content from s3 does not match designated md5 value, got: '$newMD5' expected: '$md5'");
        throw new java.lang.IllegalStateException("content from s3 does not match designated md5 value")
      }
    }

    val mapper = new ObjectMapper();
    val jsonNode = mapper.readTree(jsonContent);

    val recs = Utils.fromJson(jsonNode).asInstanceOf[Seq[Map[String,Any]]].map( node => {
      Record(
        node.get("id").getOrElse(node.get("_id")).asInstanceOf[String],
        node.get("ftime") match {
          case Some(date: Long) => new DateTime(date)
          case _ => node.get("ctime") match {
            case Some(date: Long) => new DateTime(date)
            case _ => null
          }
        },
        node.get("ctime") match {
          case Some(date: Long) => new DateTime(date)
          case _ => null
        },
        node.get("stime") match {
          case Some(date: Long) => new DateTime(date)
          case _ => null
        },
        node.get("ltime") match {
          case Some(date: Long) => new DateTime(date)
          case _ => null
        },
        node.get("mtime") match {
          case Some(date: Long) => new DateTime(date)
          case _ => null
        },
        node.get("data") match {
          case Some(data) => data
          case _ => null
        },
        node.get("tags") match {
          case Some(tags) => tags.asInstanceOf[Map[String,Any]]
          case _ => Map[String,Any]()
        }
      )
    })
    val recMtime = if( recs.isEmpty ) {
        mtime
      } else {
        recs.head.mtime
      }
    RecordSet(recs, Map("location" -> location, "mtime" -> recMtime))
  }

  override
  def update(d: Collection.Delta)(implicit req: RequestId): Collection.Delta = {
    import collection.JavaConverters._
    implicit var client = writeDynamo
    // write to S3

    val bytes = Utils.compress(Utils.toJson(d.recordSet.records))
    val uuid = Utils.uuid
    val location = s"$locationPrefix/$name.$uuid"

    var t0 = System.nanoTime()
    try {
      val is = new ByteArrayInputStream(bytes)
      val metadata = new ObjectMetadata
      metadata.setContentLength(bytes.size)
      metadata.setContentType("application/json")
      val md5 = Base64.encodeBase64String(MessageDigest.getInstance("MD5").digest(bytes)).trim
      metadata.setContentMD5(md5)
      metadata.setUserMetadata(
        Map("reqid" -> req.id, "md5" -> md5, "compressed" -> "true").asJava
      )
      val putRequest = new PutObjectRequest(bucketName, location, is, metadata)
      s3.putObject(putRequest)
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req$this s3 write lapse: ${lapse}ms")
    }
    
    val oldLocation = d.recordSet.meta.get("location") match {
      case Some(location: String) => location
      case _ => ""
    }
    
    val attrs = Map(
      "name" -> name,
      "location" -> location,
      "reqId" -> req.id
    )
    val expected = if( oldLocation.isEmpty ) Map( "name" -> None ) else Map( "location" -> oldLocation )

    DynamoDB.put(tableName, attrs, expected);
      
    if( ! oldLocation.isEmpty && autoDelete.get.toBoolean ) {
      t0 = System.nanoTime()
      try {
        s3.deleteObject(bucketName, oldLocation)
      } catch {
        case e: Exception => logger.error(s"$req$this failed to delete $oldLocation from $bucketName", e)
      } finally {
        val t1 = System.nanoTime()
        val lapse = (t1 - t0) / 1000000;
        if (logger.isInfoEnabled) logger.info(s"$req$this s3 delete lapse: ${lapse}ms")
      }
    }

    val mtime = if( d.recordSet.records.isEmpty ) {
        DateTime.now
      } else {
        d.recordSet.records.head.mtime
      }
    d.copy(recordSet = RecordSet(d.recordSet.records, d.recordSet.meta + ("location" -> location, "mtime" -> mtime)))
  }

  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean)(implicit req: RequestId): Seq[Record] = {
    throw new java.lang.UnsupportedOperationException("you cannot query the S3 Live datastore");
  }

  def remove(queryMap: Map[String, Any])(implicit req: RequestId) {
    throw new java.lang.UnsupportedOperationException("you cannot query the S3 Live datastore");
  }
    
  override def toString = "[S3CurrentDatastore " + name + "]"
}
