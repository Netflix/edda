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

import com.netflix.edda.aws.AwsBeanMapper
import com.netflix.edda.aws.AwsCollectionBuilder
import com.netflix.edda.aws.AwsClient
import com.netflix.edda.CollectionManager
import com.netflix.edda.Datastore
import com.netflix.edda.Elector
import com.netflix.edda.Utils
import com.netflix.edda.RequestId

import javax.servlet.http.HttpServlet

import org.slf4j.LoggerFactory

/** simple servlet that specifies the datastores being used and creates
  * accessors to initialize the AWS client credentials and start the collections.
  * It is recommended to create a separate Servlet if behavior changes are required
  * for special collections or datastores
  */
class BasicServer extends HttpServlet {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  implicit val req = RequestId("basicServer")

  override def init() {

    Utils.initConfiguration(System.getProperty("edda.properties","edda.properties"))

    logger.info(s"$req Staring Server")

    val electorClassName = Utils.getProperty("edda", "elector.class", "", "com.netflix.edda.mongo.MongoElector").get
    val electorClass = this.getClass.getClassLoader.loadClass(electorClassName)

    val elector = electorClass.newInstance.asInstanceOf[Elector]

    val bm = new BasicBeanMapper with AwsBeanMapper

    val awsClientFactory = (account: String) => new AwsClient(account)

    AwsCollectionBuilder.buildAll(BasicContext, awsClientFactory, bm, elector)

    if (logger.isInfoEnabled) logger.info(s"$req Starting Collections")
    CollectionManager.start()

    super.init()
  }

  override def destroy() {
    CollectionManager.stop()
    super.destroy()
  }
}
