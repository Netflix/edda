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
import com.netflix.edda.Utils

/** provides access to AWS service client objects
  *
  * @param credentials provider used to connect to AWS services
  * @param region used to select endpoint for AWS services
  */
class AwsClient(val provider: AWSCredentialsProvider, val region: String) {

  /** uses [[com.amazonaws.auth.AWSCredentials]] to create AWSCredentialsProvider
    *
    * @param credentials used to connect to AWS services
    * @param region to select endpoint for AWS services
    */
  def this(credentials: AWSCredentials, region: String) =
    this(new AWSCredentialsProvider() {def getCredentials = credentials; def refresh = {}}, region)

  /** uses [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]] to discover credentials
    *
    * @param region to select endpoint for AWS services
    */
  def this(region: String) =
    this(new DefaultAWSCredentialsProviderChain(), region)

  /** create credential from provided arguments
    *
    * @param accessKey for account access
    * @param secretKey for account access
    * @param region used to select endpoint for AWS service
    */
  def this(accessKey: String, secretKey: String, region: String) =
    this(new BasicAWSCredentials(accessKey, secretKey), region)

  /** get [[com.amazonaws.services.ec2.AmazonEC2Client]] object */
  def ec2 = {
    val client = new AmazonEC2Client(provider)
    val url = if (Utils.isAwsRegion(region)) {
      "ec2." + region + ".amazonaws.com"
    } else {
      region + ":8773/services/Eucalyptus"
    }
    client.setEndpoint(url)
    client
  }

  /** get [[com.amazonaws.services.autoscaling.AmazonAutoScalingClient]] object */
  def asg = {
    val client = new AmazonAutoScalingClient(provider)
    val url = if (Utils.isAwsRegion(region)) {
      "autoscaling." + region + ".amazonaws.com"
    } else {
      region + ":8773/services/AutoScaling"
    }
    client.setEndpoint(url)
    client
  }

  /** get [[com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient]] object */
  def elb = {
    val client = new AmazonElasticLoadBalancingClient(provider)
    val url = if (Utils.isAwsRegion(region)) {
      "elasticloadbalancing." + region + ".amazonaws.com"
    } else {
      region + ":8773/services/LoadBalancing"
    }
    client.setEndpoint(url)
    client
  }

  /** get [[com.amazonaws.services.s3.AmazonS3Client]] object */
  def s3 = {
    val client = new AmazonS3Client(provider)
    val url = if (region == "us-east-1") {
      "s3.amazonaws.com"
    } else if (Utils.isAwsRegion(region)) {
      "s3-" + region + ".amazonaws.com"
    } else {
      region + ":8773/services/Walrus"
    }
    client.setEndpoint(url)
    client
  }

  /** get [[com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient]] object */
  def identitymanagement = {
    val client = new AmazonIdentityManagementClient(provider)
    val url =     if (region == "us-gov") {
      "iam.us-gov.amazonaws.com"
    } else if (Utils.isAwsRegion(region)) {
      "iam.amazonaws.com"
    } else {
      region + ":8773/services/Euare"
    }
    client.setEndpoint(url)
    client
  }

  /** get [[com.amazonaws.services.sqs.AmazonSQSClient]] object */
  def sqs = {
    val client = new AmazonSQSClient(provider)
    client.setEndpoint("sqs." + region + ".amazonaws.com")
    client
  }

  /** get [[com.amazonaws.services.sqs.AmazonCloudWatchClient]] object */
  def cw = {
    val client = new AmazonCloudWatchClient(provider)
    val url = if (Utils.isAwsRegion(region)) {
      "monitoring." + region + ".amazonaws.com"
    } else {
      region + ":8773/services/CloudWatch"
    }
    client.setEndpoint(url)
    client
  }

   /** get [[com.amazonaws.services.route53.AmazonRoute53Client]] object */
   def route53 = {
      val client = new AmazonRoute53Client(provider)
      client.setEndpoint("route53.amazonaws.com")
      client
   }

   def rds = {
     val client = new AmazonRDSClient(provider)
     client.setEndpoint("rds.amazonaws.com")
     client
   }

   def elasticache = {
    val client = new AmazonElastiCacheClient(provider)
    client.setEndpoint("elasticache." + region + ".amazonaws.com")
    client
   }
}
