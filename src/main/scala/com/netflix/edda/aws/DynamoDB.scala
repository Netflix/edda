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

import org.slf4j.LoggerFactory

import com.netflix.edda.RequestId

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._

object DynamoDB {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  def init(tableName: String, readCap: Long, writeCap: Long)(implicit client: AmazonDynamoDBClient) {
    this.synchronized {
      val request = new DescribeTableRequest().withTableName(tableName)
      var continue = true
      while (continue) {
        try {
          val table: TableDescription = client.describeTable(request).getTable
          
          if (table.getProvisionedThroughput.getReadCapacityUnits != readCap ||
              table.getProvisionedThroughput.getWriteCapacityUnits != writeCap ) {
            val request = new UpdateTableRequest().withTableName(tableName).
            withProvisionedThroughput(
              new ProvisionedThroughput().
                withReadCapacityUnits(readCap).
                withWriteCapacityUnits(writeCap)
            )
            client.updateTable(request)
            Thread.sleep(100)
          }
          
          continue = table.getTableStatus != "ACTIVE"
        }
        catch {
          case e: ResourceNotFoundException => {
            val key = new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH)
            val keyAttr = new AttributeDefinition().withAttributeName("name").withAttributeType("S")
            val throughput = new ProvisionedThroughput().
              withReadCapacityUnits(readCap).
              withWriteCapacityUnits(writeCap)
            val request = new CreateTableRequest().
              withTableName(tableName).
              withKeySchema(key).
              withAttributeDefinitions(keyAttr).
              withProvisionedThroughput(throughput)
            client.createTable(request)
            Thread.sleep(5000)
          }
          case e: ResourceInUseException =>
            logger.error("Failed to update table", e)
          Thread.sleep(1000)
        }
      }
    }
  }

  def get(tableName: String, name: String, value: String)(implicit client: AmazonDynamoDBClient, req: RequestId): Option[Map[String,String]] = {
    import collection.JavaConverters._
    val getRequest = new GetItemRequest().withTableName(tableName).withKey(Map(name->new AttributeValue(value)).asJava).withConsistentRead(true)
    var t0 = System.nanoTime()
    val item = try {
      client.getItem(getRequest).getItem
    }
    catch {
      case e: ResourceNotFoundException => {
        logger.error(s"$req Dynamo record for $name not found", e)
        return None
      }
      case e: ProvisionedThroughputExceededException => {
        logger.error(s"$req Dynamo table $tableName is throttled", e)
        throw e
      }
      case e: Throwable => {
        logger.error(s"$req Error getting item from dynamodb: $name", e)
        throw e
      }
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req dynamo read lapse: ${lapse}ms")
    }

    if( Option(item).isEmpty ) {
      logger.error(s"$req Dynamo record for $name not found")
      return None
    }
    
    // if( Option(item).isEmpty ) {
    //     logger.error(s"$req$this Dynamo null record for $name")
    //     throw new java.lang.UnsupportedOperationException(s"Dynamo null record for $name")
    // }
    
    Some(
      item.asScala.map(pair => {
        val key = pair._1
        val attr = pair._2
        Option(attr.getS) orElse Option(attr.getN) orElse Option(attr.getB) orElse None match {
          case Some(value: String) => key -> value
          case _ => throw new java.lang.RuntimeException(s"key $key is none of [String,Number,Binary] on record $value in table $tableName")
        }
      }).toMap
    )
  }

  def toAttributeValue(value: Any): AttributeValue = {
    value match {
      case v: String => new AttributeValue().withS(v)
      case Int|Long|Float|Double => new AttributeValue().withN(value.toString)
      case _:java.lang.Integer|_:java.lang.Long|_:java.lang.Float|_:java.lang.Double => new AttributeValue().withN(value.toString)
      case v: Array[Byte] => new AttributeValue().withB(java.nio.ByteBuffer.wrap(v))
      case _ => throw new java.lang.RuntimeException(s"unable to convert ${value.getClass.getName} to DynamoDB AttributeValue")
    }
  }

  def put(tableName: String, attributes: Map[String,Any], expected: Map[String,Any] = Map())(implicit client: AmazonDynamoDBClient, req: RequestId) = {
    import collection.JavaConverters._
    val t0 = System.nanoTime()
    val request = new PutItemRequest()
    try {
      // write to DynamoDB
      client.putItem(
        request.withTableName(tableName).withItem(
          attributes.map(pair => pair._1 -> toAttributeValue(pair._2)).toMap.asJava
        ).withExpected(
          expected.map(pair => {
            val name = pair._1
            pair._2 match {
              case None => name -> new ExpectedAttributeValue().withExists(false)
              case _ => name -> new ExpectedAttributeValue().withExists(true).withValue(toAttributeValue(pair._2))
            }
          }).toMap.asJava
        )
      )
    } catch {
      case e: InternalServerErrorException => {
        logger.warn(s"$req error attempting to write $request to dynamodb: $e")
        // with this exception we have no idea if the write actually happened, so lets fetch the record and see if it matches 
        // what we just tried to write before rethrowing an exception

        // first we need to figure out what the primary key is
        // Note: this assumes only single key schema
        val key = client.describeTable(new DescribeTableRequest().withTableName(tableName)).getTable().getKeySchema.asScala.head.getAttributeName()

        val item = this.get(tableName, key, attributes(key).asInstanceOf[String])
        // compare everything in item to the arguments set in the attributes map
        if( item.isEmpty || item.get.filterKeys(attributes.keySet) != attributes ) {
          logger.error(s"$req failed to update dynamodb: $request", e)
          throw e
        }
      }
      case e: Exception => {
        logger.error(s"$req failed to update dynamodb: $request", e)
        throw e
      }
    } finally {
      val t1 = System.nanoTime()
      val lapse = (t1 - t0) / 1000000;
      if (logger.isInfoEnabled) logger.info(s"$req dynamo write lapse: ${lapse}ms")
    }
  }
}
