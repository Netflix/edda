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
package com.netflix.edda.aws

import com.netflix.edda.Utils

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.route53.AmazonRoute53Client
import com.amazonaws.services.rds.AmazonRDSClient
import com.amazonaws.services.elasticache.AmazonElastiCacheClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient

object AwsClient {

  def mkCredentialProvider(accessKey: String, secretKey: String): AWSCredentialsProvider = {
    if( accessKey.isEmpty ) {
      new DefaultAWSCredentialsProviderChain()
    } else {
      new AWSCredentialsProvider() {
        def getCredentials = new BasicAWSCredentials(accessKey, secretKey)
        def refresh = {}
      }
    }
  }

  def mkCredentialProvider(account: String): AWSCredentialsProvider = {
    AwsClient.mkCredentialProvider(
      Utils.getProperty("edda", "aws.accessKey", account, "").get,
      Utils.getProperty("edda", "aws.secretKey", account, "").get
    )
  }
}


/** provides access to AWS service client objects
  *
  * @param credentials provider used to connect to AWS services
  * @param region used to select endpoint for AWS services
  */
class AwsClient(val provider: AWSCredentialsProvider, val region: String) {

  /** uses [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentials.html com.amazonaws.auth.AWSCredentials]] to create AWSCredentialsProvider
    *
    * @param credentials used to connect to AWS services
    * @param region to select endpoint for AWS services
    */
  def this(credentials: AWSCredentials, region: String) =
    this(new AWSCredentialsProvider() {def getCredentials = credentials; def refresh = {}}, region)

  /** create credentials from config file for account
    * @param account
    */
  def this(account: String) =
    this(
      AwsClient.mkCredentialProvider(account),
      Utils.getProperty("edda", "region", account, "").get
    )

  /** create credential from provided arguments
    *
    * @param accessKey for account access
    * @param secretKey for account access
    * @param region used to select endpoint for AWS service
    */
  def this(accessKey: String, secretKey: String, region: String) =
    this(AwsClient.mkCredentialProvider(accessKey,secretKey), region)

  def endpoint( service: String ) = {
    service match {
      case "ec2" => "ec2." + region + ".amazonaws.com"
      case "autoscaling" => "autoscaling." + region + ".amazonaws.com"
      case "elasticloadbalancing" => "elasticloadbalancing." + region + ".amazonaws.com"
      case "s3" => if( region == "us-east-1" ) "s3.amazonaws.com" else "s3-" + region + ".amazonaws.com"
      case "iam" => if( region == "us-gov" ) "iam.us-gov.amazonaws.com" else "iam.amazonaws.com"
      case "sqs" => "sqs." + region + ".amazonaws.com"
      case "cloudwatch" => "monitoring." + region + ".amazonaws.com"
      case "route53" => "route53.amazonaws.com"
      case "rds" => "rds.amazonaws.com"
      case "elasticache" => "elasticache." + region + ".amazonaws.com"
      case "dynamodb" => "dynamodb." + region + ".amazonaws.com"
      case "cloudformation" => "cloudformation." + region + ".amazonaws.com"
      case _ => throw new java.lang.UnsupportedOperationException(s"no endpoint found for service $service on ${this.getClass.getName}")
    }
  }


  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/AmazonEC2Client.html com.amazonaws.services.ec2.AmazonEC2Client]] object */
  def ec2 = {
    val client = new AmazonEC2Client(provider)
    client.setEndpoint(this.endpoint("ec2"))
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/autoscaling/AmazonAutoScalingClient.html com.amazonaws.services.autoscaling.AmazonAutoScalingClient]] object */
  def asg = {
    val client = new AmazonAutoScalingClient(provider)
    client.setEndpoint(this.endpoint("autoscaling"))
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticloadbalancing/AmazonElasticLoadBalancingClient.html com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient]] object */
  def elb = {
    val client = new AmazonElasticLoadBalancingClient(provider)
    client.setEndpoint(this.endpoint("elasticloadbalancing"))
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html com.amazonaws.services.s3.AmazonS3Client]] object */
  def s3 = {
    val client = new AmazonS3Client(provider)
    client.setEndpoint(this.endpoint("s3"))
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/identitymanagement/AmazonIdentityManagementClient.html com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient]] object */
  def identitymanagement = {
    val client = new AmazonIdentityManagementClient(provider)
    client.setEndpoint(this.endpoint("iam"))
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQSClient.html com.amazonaws.services.sqs.AmazonSQSClient]] object */
  def sqs = {
    val client = new AmazonSQSClient(provider)
    client.setEndpoint(this.endpoint("sqs"))
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatchClient.html com.amazonaws.services.cloudwatch.AmazonCloudWatchClient]] object */
  def cw = {
    val client = new AmazonCloudWatchClient(provider)
    client.setEndpoint(this.endpoint("cloudwatch"))
    client
  }

   /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/route53/AmazonRoute53Client.html com.amazonaws.services.route53.AmazonRoute53Client]] object */
   def route53 = {
      val client = new AmazonRoute53Client(provider)
      client.setEndpoint(this.endpoint("route53"))
      client
   }

   def rds = {
     val client = new AmazonRDSClient(provider)
     client.setEndpoint(this.endpoint("rds"))
     client
   }

   def elasticache = {
    val client = new AmazonElastiCacheClient(provider)
    client.setEndpoint(this.endpoint("elasticache"))
    client
   }

   def dynamo = {
     val client = new AmazonDynamoDBClient(provider)
     client.setEndpoint(this.endpoint("dynamodb"))
     client
   }

   def cloudformation = {
    val client = new AmazonCloudFormationClient(provider)
    client.setEndpoint(this.endpoint("cloudformation"))
    client
   }
}
