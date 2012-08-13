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
package com.netflix.edda.resources

import java.io.ByteArrayOutputStream

import javax.ws.rs.{GET,Path}
import javax.ws.rs.core.{Response,UriInfo,Context}

import org.codehaus.jackson.{JsonEncoding,JsonGenerator}
import org.codehaus.jackson.map.MappingJsonFactory

object Resource {
    val JSON_FACTORY = new MappingJsonFactory();
}

@Path("/v1/hello")
class Resource {
    import Resource._
    @GET
    def getHello(@Context uriInfo: UriInfo): Response = {

        val baos = new ByteArrayOutputStream()
        val gen = JSON_FACTORY.createJsonGenerator(baos, JsonEncoding.UTF8)
        gen.writeStartObject()
        gen.writeStringField("message", "hello")
        gen.writeEndObject()
        gen.close()
        Response.status(Response.Status.OK).entity(baos.toString("UTF-8")).build()
    }
}
