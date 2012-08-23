package com.netflix.edda.resources

import collection.mutable.{Set => MSet}


import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType
import java.io.ByteArrayOutputStream
import java.util.Date

import com.netflix.edda.web.FieldSelectorParser
import com.netflix.edda.web.FieldSelectorExpr
import com.netflix.edda.web.MatchAnyExpr
import com.netflix.edda.CollectionManager
import com.netflix.edda.Record
import com.netflix.edda.Utils

import org.codehaus.jackson.JsonEncoding.UTF8
import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.util.DefaultPrettyPrinter
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.map.MappingJsonFactory

import org.slf4j.{Logger, LoggerFactory}

import org.joda.time.DateTime


class RecordCollectionServlet extends HttpServlet {

    private val logger = LoggerFactory.getLogger(getClass)

    private val factory = new MappingJsonFactory

    private val collectionPathRx = """^(/[^/]+/[^/:;]+)/?([^/:;]+)?((?:;[^/;:]*(?:=[^/;:]+)?)*)(:.*)?""".r

    private def fail(res: HttpServletResponse, message: String, code: Int) = {
        res.setContentType(MediaType.APPLICATION_JSON)
        val output = res.getOutputStream
        val gen = factory.createJsonGenerator(output, UTF8)
        res.setStatus(code);
        gen.writeStartObject
        gen.writeNumberField("code", code)
        gen.writeStringField("message", message)
        gen.writeEndObject
        gen.close
    }

    private def unseen(id: String, seen: MSet[String]): Boolean = {
        val in = seen.contains(id)
        if ( !in ) seen += id
        return !in;
    }

    private def unique(recs: List[Record], details: ReqDetails): List[Record] = {
        if( details.metaArgs.contains("_all") ) return recs
        var seen: MSet[String] = MSet()
        return recs.filter( r => unseen(r.id, seen) )
    }

    private def makeQuery(details: ReqDetails ): Map[String,Any] = {
        var query: Map[String,Any] = Map()
        
        val prefix = if(details.meta) "" else "data."

        details.matrixArgs.foreach {
            case (k,null) => query += prefix + k -> Map("$nin" -> List(null,""))
            case (k,v)    => query += prefix + k -> v
        }

        if( details.metaArgs.contains("_at") || details.live ) {
            query += "stime" -> Map("$lte" -> details.at)
            query += "$or"   -> List(
                Map("ltime" -> null),
                Map("ltime" -> Map("$gte" -> details.at))
            )
        }

        if( details.updated ) {
            val since = Map("stime" -> Map("$gte" -> details.since))
            val until = Map("stime" -> Map("$lte" -> details.until))

            // if we only want updated then we dont care if something
            // was alive after _since or before _until
            if( details.metaArgs.contains("_since") && details.metaArgs.contains("_until") ) {
                query += "$and" -> List(since,until)
            }
            else if( details.metaArgs.contains("_since") ) {
                query ++= since
            }
            else if( details.metaArgs.contains("_until") ) {
                query ++= until
            }
        }
        else {
            val since = 
                Map("$or"   -> List(
                    Map("stime" -> Map("$gte" -> details.since)),
                    Map("ltime" -> null),
                    Map("ltime" -> Map("$gt"  -> details.since))
                ))
            val until = 
                Map("$or"   -> List(
                    Map("stime" -> Map("$lte" -> details.until)),
                    Map("ltime" -> Map("$lt"  -> details.until))
                ))
            if( details.metaArgs.contains("_since") && details.metaArgs.contains("_until") ) {
                query += "$and" -> List(since,until)
            }
            else if ( details.metaArgs.contains("_since") ) {
                query ++= since
            }
            else if ( details.metaArgs.contains("_until") ) {
                query ++= until
            }
        }
        return query
    }

    object ReqDetails {
        def apply(req: HttpServletRequest, res: HttpServletResponse, id: String, matrixStr: String, exprStr: String): ReqDetails = {
            val args: Map[String,String] = matrixStr match {
                case m if m == null || m == "" => Map()
                // skip null/or empty matrix (ie ";;a=b"), also map value null to matrix args missing value
                case _    =>
                    matrixStr.tail.split(";").map(_ split "=").collect({
                        case Array(k,v) => (k,v)
                        case Array(m) if m != "" => (m,null)
                        case v: Array[String] if v.size > 2 => (v.head, v.tail.fold("")(_+"="+_))
                    }).toMap
            }
            val expr = if (exprStr == null) MatchAnyExpr
                       else FieldSelectorParser.parse(exprStr)

            val metaArgs   = args.filter(t => t._1.head == '_')
            val matrixArgs = args.filter(t => t._1.head != '_')
            return new ReqDetails(req,res,id,metaArgs,matrixArgs,expr)
        }
    }

    case class ReqDetails(req: HttpServletRequest, res: HttpServletResponse, id: String, metaArgs: Map[String,String], matrixArgs: Map[String,String], expr: FieldSelectorExpr) {
        lazy val baos = {
            val v = new ByteArrayOutputStream()
            if( cb != null ) {
                v.write((cb + "(").getBytes, 0, (cb + "(").size);
            }
            v
        }
        
        lazy val gen = {
            val jg = factory.createJsonGenerator(baos, UTF8)
            val dpp = new DefaultPrettyPrinter;
            dpp.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter)
            if( pp ) jg.setPrettyPrinter(dpp)
            jg
        }

        val path = req.getRequestURI.drop(req.getServletPath.length)

        var at      = timeArg(metaArgs.get("_at"))
        var since   = timeArg(metaArgs.get("_since"))
        var until   = timeArg(metaArgs.get("_until"))
        val meta    = boolArg(metaArgs.get("_meta"))
        val all     = boolArg(metaArgs.get("_all")) || (id != null && (metaArgs.contains("_since") || metaArgs.contains("_until")))
        val diff: Option[String] = metaArgs.get("_diff")
        val pp      = diff != None || boolArg(metaArgs.get("_pp"));
        val live    = boolArg(metaArgs.get("_live"));
        val updated = boolArg(metaArgs.get("_updated"));
        val cb      = if(metaArgs.contains("_callback")) metaArgs("_callback") else null
        val limit   = if(id != null && !all) 1 else intArg(metaArgs.get("_limit"))
        val expand  = id != null || meta || all || boolArg(metaArgs.get("_expand"))

        // if user requested pretty-print then reformat
        // the datetimes to be human readable, otherwise
        // use the pass-thru formatter
        val formatter = if( pp ) Utils.dateFormatter(_) else nopFormatter(_)

        def nopFormatter(arg: Any): Any = {
            arg
        }

        var timeTravelling = all || metaArgs.contains("_at") || metaArgs.contains("_since") || live
        def send() {
            gen.close
            if( cb != null ) {
                baos.write(')')
            }
            baos.writeTo(res.getOutputStream)
        }
        
        private def boolArg(v: Option[String]): Boolean = {
            v match {
                case Some(t) if t == "1" => true
                case Some(null)          => true
                case _                   => false
            }
        }
        
        private def timeArg(v: Option[String]): DateTime = {
            v match {
                case Some(t) => new DateTime(t.toLong, org.joda.time.DateTimeZone.UTC)
                case None => DateTime.now()
            }
        }
        
        private def intArg(v: Option[String]): Int = {
            v match {
                case Some(null) => 0
                case Some(t) => t.toInt
                case None => 0
            }
        }
    }
        
    def writeMaybeMeta(r: Record, details: ReqDetails) {
        val data = 
            if(details.meta)
                details.expr.select(r.toMap).getOrElse(Map())
            else
                details.expr.select(r.data).getOrElse(Map())

        Utils.toJson(details.gen, data, details.formatter)
    }
    
    def dispatch(collName: String, details: ReqDetails) {
        collName match {
            case c if CollectionManager.names.contains(c) => handleBasicCollection(collName,details)
            case _ => fail(details.res, "invalid collection: " + collName, HttpServletResponse.SC_BAD_REQUEST)
        }
    }
    
    def handleBasicCollection(collName: String, details: ReqDetails): Unit = {
        var recs = selectRecords(collName,details)
        if( details.id != null && !details.all) {
            if( recs.isEmpty ) {
                if( !details.timeTravelling ) {
                    var recs = selectRecords(collName,details.copy(metaArgs=details.metaArgs ++ Map("_live" -> null, "_since" -> "0","_limit" -> "1")));
                    if( !recs.isEmpty ) {
                        fail(details.res,"record \"" + details.id + "\" is no longer valid in collection " + collName + ". Use _at, _since or _all arguments to fetch historical records.  Last seen at " + recs.head.stime.getMillis, HttpServletResponse.SC_GONE)
                        return;
                    }
                }
                fail(details.res,"record \"" + details.id + "\" not found in collection " + collName, HttpServletResponse.SC_NOT_FOUND)
                return;
            }
        }
        else if( details.diff == None ) details.gen.writeStartArray
        if( details.diff != None && details.id != null ) {
            if( recs.size == 1 ) {
                fail(details.res, "_diff requires at least 2 documents, only 1 found", HttpServletResponse.SC_BAD_REQUEST)
                return
            }
            var diff = Utils.diffRecords(
                recs,
                details.diff.collect({
                    case x: String => x.toInt
                }),
                details.req.getServletPath + collName
            )
            val bytes = diff.getBytes
            details.baos.write(bytes, 0, bytes.size)
            
        }
        else {
            details.expand match {
                case true => recs.foreach(writeMaybeMeta(_, details))
                case _    => recs.map( r => r.id ).foreach(details.gen.writeString(_))
            }
        }
        if( details.diff == None && (details.id == null || details.all) ) details.gen.writeEndArray
        details.send
    }

    def selectRecords(collName: String, details: ReqDetails): List[Record] = {
        val coll = CollectionManager.get(collName).get
        val query = if(details.id != null) makeQuery(details) + ("id" -> details.id) else makeQuery(details)
        return unique(coll.query(query, details.limit, details.live), details)
    }

    override def doGet(req: HttpServletRequest, res: HttpServletResponse) = {
        val path = req.getRequestURI.drop(req.getServletPath.length)
        path match {
            case collectionPathRx(collName, id, matrixStr, exprStr) => {
                val details = ReqDetails(req,res,id,matrixStr,exprStr)
                if( details.id == null && details.diff != None ) {
                    fail(res, "_diff argument requires use of resource id: " + req.getServletPath + collName + "/<id>", HttpServletResponse.SC_BAD_REQUEST)
                }
                else {
                    if( details.cb != null ) {
                        res.setContentType("application/javascript")
                    }
                    else if( details.diff != None ) {
                        res.setContentType(MediaType.TEXT_PLAIN)
                    }
                    else res.setContentType(MediaType.APPLICATION_JSON)
                    dispatch(collName, details);
                }
            }
            case _ => fail(res, "invalid path: " + path, HttpServletResponse.SC_BAD_REQUEST)
        }
    }
}
