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

import com.netflix.edda.Record

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient
import com.amazonaws.services.s3.AmazonS3Client

trait AwsClientComponent {
  val awsClient: AwsClient
}

class AwsClient(val credentials: AWSCredentials, val region: String) {

  def this(region: String) =
    this(new DefaultAWSCredentialsProviderChain().getCredentials(), region)

  def this(accessKey: String, secretKey: String, region: String) =
    this(new BasicAWSCredentials(accessKey, secretKey), region)

  def ec2 = {
    val client = new AmazonEC2Client(credentials)
    client.setEndpoint("ec2." + region + ".amazonaws.com")
    client
  }

  def asg = {
    val client = new AmazonAutoScalingClient(credentials)
    client.setEndpoint("autoscaling." + region + ".amazonaws.com")
    client
  }

  def elb = {
    val client = new AmazonElasticLoadBalancingClient(credentials)
    client.setEndpoint("elasticloadbalancing." + region + ".amazonaws.com")
    client
  }

  def s3 = {
    val client = new AmazonS3Client(credentials)
    if (region == "us-east-1")
      client.setEndpoint("s3.amazonaws.com")
    else
      client.setEndpoint("s3-" + region + ".amazonaws.com")
    client
  }
}
