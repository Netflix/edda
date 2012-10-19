/**
 * Copyright 2012 Netflix, Inc.
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

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.s3.AmazonS3Client

/** provides access to AWS service client objects
  *
  * @param credentials used to connect to AWS services
  * @param region used to select endpoint for AWS services
  */
class AwsClient(val credentials: AWSCredentials, val region: String) {

  /** uses [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]] to discover credentials
    *
    * @param region to select endpoint for AWS services
    */
  def this(region: String) =
    this(new DefaultAWSCredentialsProviderChain().getCredentials, region)

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
    val client = new AmazonEC2Client(credentials)
    client.setEndpoint("ec2." + region + ".amazonaws.com")
    client
  }

  /** get [[com.amazonaws.services.autoscaling.AmazonAutoScalingClient]] object */
  def asg = {
    val client = new AmazonAutoScalingClient(credentials)
    client.setEndpoint("autoscaling." + region + ".amazonaws.com")
    client
  }

  /** get [[com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient]] object */
  def elb = {
    val client = new AmazonElasticLoadBalancingClient(credentials)
    client.setEndpoint("elasticloadbalancing." + region + ".amazonaws.com")
    client
  }

  /** get [[com.amazonaws.services.s3.AmazonS3Client]] object */
  def s3 = {
    val client = new AmazonS3Client(credentials)
    if (region == "us-east-1")
      client.setEndpoint("s3.amazonaws.com")
    else
      client.setEndpoint("s3-" + region + ".amazonaws.com")
    client
  }
}
