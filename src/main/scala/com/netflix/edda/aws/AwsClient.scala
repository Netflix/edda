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

class AwsClient(credentials: AWSCredentials, region: String) {
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
