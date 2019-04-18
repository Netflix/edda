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

import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import java.util.{LinkedList => JList}
import java.util.Date
import com.amazonaws.services.autoscaling.model.AutoScalingGroup
import com.amazonaws.services.autoscaling.model.Instance
import com.amazonaws.services.autoscaling.model.TagDescription

import org.scalatest.FunSuite

class BasicBeanMapperTest extends FunSuite {
  val logger = LoggerFactory.getLogger(getClass)
  test("fromBean") {
    val mapper = new BasicBeanMapper

    val asg = new AutoScalingGroup
    asg.setVPCZoneIdentifier("")
    asg.setAutoScalingGroupARN("ARN")
    asg.setAutoScalingGroupName("asgName")
    val azs = new JList[String]()
    azs.add("us-east-1c")
    asg.setAvailabilityZones(azs)
    asg.setCreatedTime(new Date(0))
    asg.setDefaultCooldown(10)
    asg.setDesiredCapacity(2)
    asg.setHealthCheckGracePeriod(600)
    asg.setHealthCheckType("EC2")

    val inst1 = new Instance
    inst1.setAvailabilityZone("us-east-1c")
    inst1.setHealthStatus("Healthy")
    inst1.setInstanceId("i-0123456789")
    inst1.setLaunchConfigurationName("launchConfigName")
    inst1.setLifecycleState("InService")
    val instances = new JList[Instance]()
    instances.add(inst1)
    asg.setInstances(instances)

    asg.setLaunchConfigurationName("launchConfigName")
    val elbs = new JList[String]()
    elbs.add("elbName")
    asg.setLoadBalancerNames(elbs)
    asg.setMaxSize(2)
    asg.setMinSize(2)

    val tag = new TagDescription
    tag.setKey("tagName")
    tag.setValue("tagValue")
    val tags = new JList[TagDescription]()
    tags.add(tag)
    asg.setTags(tags)


    val expected = Map(
      "terminationPolicies" -> List(),
      "healthCheckGracePeriod" -> 600,
      "tags" -> List(
        Map(
          "resourceId" -> null,
          "resourceType" -> null,
          "key" -> "tagName",
          "class" -> "com.amazonaws.services.autoscaling.model.TagDescription",
          "propagateAtLaunch" -> null,
          "value" -> "tagValue"
        )
      ),
      "healthCheckType" -> "EC2",
      "autoScalingGroupARN" -> "ARN",
      "placementGroup" -> null,
      "instances" -> List(
        Map(
          "instanceId" -> "i-0123456789",
          "healthStatus" -> "Healthy",
          "availabilityZone" -> "us-east-1c",
          "launchTemplate" -> null,
          "protectedFromScaleIn" -> null,
          "lifecycleState" -> "InService",
          "class" -> "com.amazonaws.services.autoscaling.model.Instance",
          "launchConfigurationName" -> "launchConfigName"
        )
      ),
      "launchTemplate" -> null,
      "VPCZoneIdentifier" -> "",
      "defaultCooldown" -> 10,
      "loadBalancerNames" -> List("elbName"),
      "createdTime" -> new DateTime(0),
      "suspendedProcesses" -> List(),
      "status" -> null,
      "desiredCapacity" -> 2,
      "class" -> "com.amazonaws.services.autoscaling.model.AutoScalingGroup",
      "enabledMetrics" -> List(),
      "newInstancesProtectedFromScaleIn" -> null,
      "maxSize" -> 2,
      "availabilityZones" -> List("us-east-1c"),
      "autoScalingGroupName" -> "asgName",
      "minSize" -> 2,
      "launchConfigurationName" -> "launchConfigName",
      "targetGroupARNs" -> List()
    )

    expectResult(expected) {
      mapper.fromBean(asg)
    }

    val pf1: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj: com.amazonaws.services.autoscaling.model.TagDescription, "value", Some(x: Any)) if obj.getKey == "tagName" => None
    }
    mapper.addKeyMapper(pf1)

    expectResult(expected + ("tags" -> List(expected("tags").asInstanceOf[List[Map[String, Any]]].head - "value"))) {
      mapper.fromBean(asg)
    }

    val pf2: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj: com.amazonaws.services.autoscaling.model.TagDescription, "value", Some(x: Any)) if obj.getKey == "tagName" => Some("newValue")
    }
    mapper.addKeyMapper(pf2)

    expectResult(expected + ("tags" -> List(expected("tags").asInstanceOf[List[Map[String, Any]]].head + ("value" -> "newValue")))) {
      mapper.fromBean(asg)
    }

    // reset keyMapper
    val pf3: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj, key, opt) => opt
    }
    mapper.addKeyMapper(pf3)

    // create object mapper to trim out "instances"
    val objMapper1: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj, "instances", value) => None
    }
    mapper.addKeyMapper(objMapper1)

    expectResult(expected - "instances") {
      mapper.fromBean(asg)
    }

    // create object mapper to replace instances with empty list
    val objMapper2: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj, "instances", value) => Some(List[Instance]())
    }
    mapper.addKeyMapper(objMapper2)

    expectResult(expected + ("instances" -> List[Instance]())) {
      mapper.fromBean(asg)
    }
  }
}
