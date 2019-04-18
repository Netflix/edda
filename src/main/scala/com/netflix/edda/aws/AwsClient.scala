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

import com.netflix.edda.Utils

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider

import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.elasticloadbalancingv2.{AmazonElasticLoadBalancingClient => AmazonElasticLoadBalancingV2Client}
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.route53.AmazonRoute53Client
import com.amazonaws.services.rds.AmazonRDSClient
import com.amazonaws.services.elasticache.AmazonElastiCacheClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest

object AwsClient {
  def mkCredentialProvider(accessKey: String, secretKey: String, arn: String): AWSCredentialsProvider = {
    val provider = if( accessKey.isEmpty ) {
      new DefaultAWSCredentialsProviderChain()
    } else {
      new AWSCredentialsProvider() {
        def getCredentials = new BasicAWSCredentials(accessKey, secretKey)
        def refresh = {}
      }
    }
    if (arn.isEmpty) {
        provider
    } else {
        new STSAssumeRoleSessionCredentialsProvider(provider, arn, "edda")
    }
  }
}


/** provides access to AWS service client objects
  *
  * @param credentials provider used to connect to AWS services
  * @param region used to select endpoint for AWS services
  */
class AwsClient(val provider: AWSCredentialsProvider, val region: String) {

  var account = ""

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
      AwsClient.mkCredentialProvider(
        Utils.getProperty("edda", "aws.accessKey", account, "").get,
        Utils.getProperty("edda", "aws.secretKey", account, "").get,
        Utils.getProperty("edda", "aws.assumeRoleArn", account, "").get
      ),
      Utils.getProperty("edda", "region", account, "").get
    )

  /** create credential from provided arguments
    *
    * @param accessKey for account access
    * @param secretKey for account access
    * @param region used to select endpoint for AWS service
    */
  def this(accessKey: String, secretKey: String, region: String) =
    this(AwsClient.mkCredentialProvider(accessKey,secretKey, ""), region)


  /** generate a resource arn */
  def arn(resourceAPI: String, resourceType: String, resourceName: String): String = {
    "arn:aws:" + resourceAPI + ":" + region + ":" + account + ":" + resourceType + arnSeperator(resourceType) + resourceName
  }

  def arnSeperator(t:String): String = if (t == "loadbalancer") "/" else ":"

  def getAccountNum(): String = {
    var stsClient = new AWSSecurityTokenServiceClient()
    stsClient.getCallerIdentity(new GetCallerIdentityRequest()).getAccount()
  }

  def loadAccountNum() {
    this.setAccountNum(this.getAccountNum)
  }

  def setAccountNum(accountNumber: String) {
    this.account = accountNumber
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/AmazonEC2Client.html com.amazonaws.services.ec2.AmazonEC2Client]] object */
  def ec2 = {
    val client = new AmazonEC2Client(provider)
    client.setEndpoint("ec2." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/autoscaling/AmazonAutoScalingClient.html com.amazonaws.services.autoscaling.AmazonAutoScalingClient]] object */
  def asg = {
    val client = new AmazonAutoScalingClient(provider)
    client.setEndpoint("autoscaling." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticloadbalancing/AmazonElasticLoadBalancingClient.html com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient]] object */
  def elb = {
    val client = new AmazonElasticLoadBalancingClient(provider)
    client.setEndpoint("elasticloadbalancing." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticloadbalancingv2/AmazonElasticLoadBalancingClient.html com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClient]] object */
  def elbv2 = {
    val client = new AmazonElasticLoadBalancingV2Client(provider)
    client.setEndpoint("elasticloadbalancing." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html com.amazonaws.services.s3.AmazonS3Client]] object */
  def s3 = {
    val client = new AmazonS3Client(provider)
    if (region == "us-east-1")
      client.setEndpoint("s3.amazonaws.com")
    else
      client.setEndpoint("s3-" + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/identitymanagement/AmazonIdentityManagementClient.html com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient]] object */
  def identitymanagement = {
    val client = new AmazonIdentityManagementClient(provider)
    if (region == "us-gov")
      client.setEndpoint("iam.us-gov.amazonaws.com")
    else
      client.setEndpoint("iam.amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQSClient.html com.amazonaws.services.sqs.AmazonSQSClient]] object */
  def sqs = {
    val client = new AmazonSQSClient(provider)
    client.setEndpoint("sqs." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatchClient.html com.amazonaws.services.cloudwatch.AmazonCloudWatchClient]] object */
  def cw = {
    val client = new AmazonCloudWatchClient(provider)
    client.setEndpoint("monitoring." + region + ".amazonaws.com")
    client
  }

   /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/route53/AmazonRoute53Client.html com.amazonaws.services.route53.AmazonRoute53Client]] object */
   def route53 = {
      val client = new AmazonRoute53Client(provider)
      client.setEndpoint("route53.amazonaws.com")
      client
   }

   def rds = {
     val client = new AmazonRDSClient(provider)
     client.setEndpoint("rds." + region + ".amazonaws.com")
     client
   }

   def elasticache = {
    val client = new AmazonElastiCacheClient(provider)
    client.setEndpoint("elasticache." + region + ".amazonaws.com")
    client
   }

   def dynamo = {
     val client = new AmazonDynamoDBClient(provider)
     client.setEndpoint("dynamodb." + region + ".amazonaws.com")
     client
   }

   def cloudformation = {
    val client = new AmazonCloudFormationClient(provider)
    client.setEndpoint("cloudformation." + region + ".amazonaws.com")
    client
   }
}
