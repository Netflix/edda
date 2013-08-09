/*
 *
 *  Copyright 2012 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.edda.basic

import com.netflix.edda.aws.AwsBeanMapper
import com.netflix.edda.aws.AwsCollectionBuilder
import com.netflix.edda.aws.AwsClient
import com.netflix.edda.CollectionManager
import com.netflix.edda.Datastore
import com.netflix.edda.Elector
import com.netflix.edda.Utils

import javax.servlet.http.HttpServlet

import org.slf4j.LoggerFactory

/** simple servlet that specifies the datastores being used and creates
  * accessors to initialize the AWS client credentials and start the collections.
  * It is recommended to create a separate Servlet if behavior changes are required
  * for special collections or datastores
  */
class BasicServer extends HttpServlet {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  override def init() {

    Utils.initConfiguration(System.getProperty("edda.properties","edda.properties"))

    logger.info("Staring Server")

    val datastoreClassName = Utils.getProperty("edda", "datastore.class", "", "com.netflix.edda.mongo.MongoDatastore").get
    val datastoreClass = this.getClass.getClassLoader.loadClass(datastoreClassName)
    val datastoreCtor = datastoreClass.getConstructor(classOf[String])

    val dsFactory = (name: String) => Some(datastoreCtor.newInstance(name).asInstanceOf[Datastore])

    val electorClassName = Utils.getProperty("edda", "elector.class", "", "com.netflix.edda.mongo.MongoElector").get
    val electorClass = this.getClass.getClassLoader.loadClass(electorClassName)

    val elector = electorClass.newInstance.asInstanceOf[Elector]

    val bm = new BasicBeanMapper with AwsBeanMapper

    val awsClientFactory = (account: String) => {
      Utils.getProperty("edda", "aws.accessKey", account, "").get match {
        case v if v.isEmpty => new AwsClient(Utils.getProperty("edda", "region", account, "").get)
        case accessKey => new AwsClient(
          accessKey,
          Utils.getProperty("edda", "aws.secretKey", account, "").get,
          Utils.getProperty("edda", "region", account, "").get)
      }
    }

    AwsCollectionBuilder.buildAll(BasicContext, awsClientFactory, bm, elector, dsFactory)

    if (logger.isInfoEnabled) logger.info("Starting Collections")
    CollectionManager.start()

    super.init()
  }

  override def destroy() {
    CollectionManager.stop()
    super.destroy()
  }
}
