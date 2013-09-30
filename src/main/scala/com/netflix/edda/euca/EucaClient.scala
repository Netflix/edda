/**
 * Copyright 2013 Netflix, Inc.
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

package com.netflix.edda.euca

import com.netflix.edda.Utils
import com.netflix.edda.aws.AwsClient 

class EucaClient(val account: String) extends AwsClient(account) {
   lazy val host = Utils.getProperty("edda", "euca.host", account, "localhost:8773").get

  override def endpoint(service: String) = {
    service match {
      case "ec2" => s"$host/services/Eucalpytus"
      case "autoscaling" => s"$host/services/AutoScaling"
      case "elasticloadbalancing" => s"$host/services/LoadBalancing"
      case "s3" => s"$host/services/Walrus"
      case "iam" => s"$host/services/Euare"
      case "cloudwatch" => s"$host/services/CloudWatch"
      case _ => throw new java.lang.UnsupportedOperationException(s"no endpoint found for service $service on ${this.getClass.getName}")
    }
  }
}
