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
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider

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

//for proxy setup
import com.amazonaws.ClientConfiguration
import org.slf4j.LoggerFactory

object AwsClient {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  def mkCredentialProvider(accessKey: String, secretKey: String, arn: String): AWSCredentialsProvider = {
    val provider = if (accessKey.isEmpty) {
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
  
  def toInt(s: String): Int = util.Try(s.toInt).getOrElse(0)
  
  def mkClientConfiguration(): ClientConfiguration = {
    val proxyHost = Utils.getProperty("edda", "proxyHost", "", "").get
    val proxyPort = Utils.getProperty("edda", "proxyPort", "", "").get
    val proxyUserName = Utils.getProperty("edda", "proxyUserName", "", "").get
    val proxyPassword = Utils.getProperty("edda", "proxyPassword", "", "").get
    val clientConfiguration = new ClientConfiguration()
    if(proxyHost.isEmpty){
      logger.info("proxyHost is Empty")
      clientConfiguration
    }
    else{
      logger.info("proxyHost is Not Empty")
      logger.info("proxyHost value is {}",proxyHost)
      clientConfiguration.setProxyHost(proxyHost)
      if(!proxyPort.isEmpty){
        logger.info("proxyPort is Not Empty")
        clientConfiguration.setProxyPort(toInt(proxyPort))
      }
      if(!proxyUserName.isEmpty){
        clientConfiguration.setProxyUsername(proxyUserName)
      }
      if(!proxyPassword.isEmpty){
        clientConfiguration.setProxyPassword(proxyPassword)
      }
      clientConfiguration
    }
  }
}

/**
 * provides access to AWS service client objects
 *
 * @param credentials provider used to connect to AWS services
 * @param region used to select endpoint for AWS services
 */
class AwsClient(val provider: AWSCredentialsProvider, val region: String) {

  /**
   * uses [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentials.html com.amazonaws.auth.AWSCredentials]] to create AWSCredentialsProvider
   *
   * @param credentials used to connect to AWS services
   * @param region to select endpoint for AWS services
   */
  def this(credentials: AWSCredentials, region: String) =
    this(new AWSCredentialsProvider() { def getCredentials = credentials; def refresh = {} }, region)

  /**
   * create credentials from config file for account
   * @param account
   */
  def this(account: String) =
    this(
      AwsClient.mkCredentialProvider(
        Utils.getProperty("edda", "aws.accessKey", account, "").get,
        Utils.getProperty("edda", "aws.secretKey", account, "").get,
        Utils.getProperty("edda", "aws.assumeRoleArn", account, "").get),
      Utils.getProperty("edda", "region", account, "").get)

  /**
   * create credential from provided arguments
   *
   * @param accessKey for account access
   * @param secretKey for account access
   * @param region used to select endpoint for AWS service
   */
  def this(accessKey: String, secretKey: String, region: String) =
    this(AwsClient.mkCredentialProvider(accessKey, secretKey, ""), region)

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/AmazonEC2Client.html com.amazonaws.services.ec2.AmazonEC2Client]] object */
  def ec2 = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonEC2Client(provider,clientConfig)
    client.setEndpoint("ec2." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/autoscaling/AmazonAutoScalingClient.html com.amazonaws.services.autoscaling.AmazonAutoScalingClient]] object */
  def asg = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonAutoScalingClient(provider,clientConfig)
    client.setEndpoint("autoscaling." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticloadbalancing/AmazonElasticLoadBalancingClient.html com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient]] object */
  def elb = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonElasticLoadBalancingClient(provider,clientConfig)
    client.setEndpoint("elasticloadbalancing." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html com.amazonaws.services.s3.AmazonS3Client]] object */
  def s3 = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonS3Client(provider,clientConfig)
    if (region == "us-east-1")
      client.setEndpoint("s3.amazonaws.com")
    else
      client.setEndpoint("s3-" + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/identitymanagement/AmazonIdentityManagementClient.html com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient]] object */
  def identitymanagement = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonIdentityManagementClient(provider,clientConfig)
    if (region == "us-gov")
      client.setEndpoint("iam.us-gov.amazonaws.com")
    else
      client.setEndpoint("iam.amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQSClient.html com.amazonaws.services.sqs.AmazonSQSClient]] object */
  def sqs = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonSQSClient(provider,clientConfig)
    client.setEndpoint("sqs." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/AmazonCloudWatchClient.html com.amazonaws.services.cloudwatch.AmazonCloudWatchClient]] object */
  def cw = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonCloudWatchClient(provider,clientConfig)
    client.setEndpoint("monitoring." + region + ".amazonaws.com")
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/route53/AmazonRoute53Client.html com.amazonaws.services.route53.AmazonRoute53Client]] object */
  def route53 = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonRoute53Client(provider,clientConfig)
    client.setEndpoint("route53.amazonaws.com")
    client
  }

  def rds = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonRDSClient(provider,clientConfig)
    client.setEndpoint("rds.amazonaws.com")
    client
  }

  def elasticache = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonElastiCacheClient(provider,clientConfig)
    client.setEndpoint("elasticache." + region + ".amazonaws.com")
    client
  }

  def dynamo = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonDynamoDBClient(provider,clientConfig)
    client.setEndpoint("dynamodb." + region + ".amazonaws.com")
    client
  }

  def cloudformation = {
    //add clientConfig parameter for ability to run from behind a proxy
    val clientConfig = AwsClient.mkClientConfiguration()
    val client = new AmazonCloudFormationClient(provider,clientConfig)
    client.setEndpoint("cloudformation." + region + ".amazonaws.com")
    client
  }
}
