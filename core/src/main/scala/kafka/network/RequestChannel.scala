/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.{Counter, Histogram, Meter}
import kafka.metrics.KafkaMetricsGroup
import kafka.network
import kafka.server.{BrokerMetadataStats, KafkaConfig, Observer}
import kafka.utils.{Logging, NotNothing, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ObjectSerializationCache}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import java.util
import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Metrics(enabledApis: Iterable[ApiKeys], config: KafkaConfig) {
    def this(scope: ListenerType, config: KafkaConfig) = {
      this(ApiKeys.apisForListener(scope).asScala, config)
    }

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    // map[acks, map[requestSize, metricName]]
    // this is used to create metrics for produce requests of different size and acks
    val produceRequestAcksSizeMetricNameMap = getProduceRequestAcksSizeMetricNameMap()
    // map[responseSize, metricName]
    // this is used to create metrics for fetch requests of different response size
    val consumerFetchRequestSizeMetricNameMap = getConsumerFetchRequestAcksSizeMetricNameMap

    (enabledApis.map(_.name) ++
      Seq(RequestMetrics.MetadataAllTopics) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName) ++
      getConsumerFetchRequestSizeMetricNames ++
      getProduceRequestAcksSizeMetricNames)
      .foreach { name => metricsMap.put(name, new RequestMetrics(name, config))
    }

    def apply(metricName: String): RequestMetrics = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }

    // generate map[request/responseSize, metricName] for requests of different size
    def getRequestSizeMetricNameMap(requestType: String): util.TreeMap[Int, String] = {
      val buckets = config.requestMetricsSizeBuckets
      // get size and corresponding term to be used in metric name
      // [0,1,10,50,100] => Seq((0, "Produce0To1Mb"), (1, "Produce1To10Mb"), (10, "Produce10To50Mb"),
      // (50, "Produce50To100Mb"),(100, "Produce100MbGreater"))
      RequestMetrics.getBucketBoundaryObjectMap(buckets, requestType, "Mb", name => name, false)
    }

    // generate map[acks, map[requestSize, metricName]] for produce requests of different request size and acks
    private def getProduceRequestAcksSizeMetricNameMap():Map[Int, util.TreeMap[Int, String]] = {
      val produceRequestAcks = Seq((0, "0"), (1, "1"), (-1, "All"))
      val requestSizeMetricNameMap = getRequestSizeMetricNameMap(ApiKeys.PRODUCE.name)

      val ackSizeMetricNames = for(ack <- produceRequestAcks) yield {
        val treeMap = new util.TreeMap[Int, String]
        requestSizeMetricNameMap.asScala.map({case(size, name) => treeMap.put(size, s"${name}Acks${ack._2}")})
        (ack._1, treeMap)
      }
      ackSizeMetricNames.toMap
    }

    // generate map[responseSize, metricName] for consumerFetch requests of different request size and acks
    private def getConsumerFetchRequestAcksSizeMetricNameMap():util.TreeMap[Int, String] = {
      getRequestSizeMetricNameMap(RequestMetrics.consumerFetchMetricName)
    }

    // get all the metric names for produce requests of different acks and size
    def getProduceRequestAcksSizeMetricNames : Seq[String] = {
      produceRequestAcksSizeMetricNameMap.values.toSeq.map(a => a.values.asScala.toSeq).flatten
    }

    // get all the metric names for fetch requests of different size
    def getConsumerFetchRequestSizeMetricNames : Seq[String] = {
      consumerFetchRequestSizeMetricNameMap.values.asScala.toSeq
    }

    // get the metric name for a given request/response size
    // the bucket is [left, right)
    def getRequestSizeBucketMetricName(sizeMetricNameMap: util.TreeMap[Int, String], sizeBytes: Long): String = {
      val sizeMb = sizeBytes / 1024 / 1024
      RequestMetrics.getBucketObject(sizeMetricNameMap, sizeMb.toInt)
    }
  }

  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var apiThrottleTimeMs = 0L
    @volatile var temporaryMemoryBytes = 0L
    @volatile var responseBytes = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)

    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    // This is constructed on creation of a Request so that the JSON representation is computed before the request is
    // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).
    val requestLog: Option[JsonNode] =
      if (RequestChannel.isRequestLoggingEnabled) Some(RequestConvertToJson.request(loggableRequest))
      else None

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def sizeInBytes: Int = header.size(new ObjectSerializationCache) + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      envelope match {
        case Some(request) =>
          val envelopeResponse = if (abstractResponse.errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            // Since it's a NOT_CONTROLLER error response, we need to make envelope response with NOT_CONTROLLER error
            // to notify the requester (i.e. BrokerToControllerRequestThread) to update active controller
            new EnvelopeResponse(new EnvelopeResponseData()
              .setErrorCode(Errors.NOT_CONTROLLER.code()))
          } else {
            val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
            new EnvelopeResponse(responseBytes, Errors.NONE)
          }
          request.context.buildResponseSend(envelopeResponse)
        case None =>
          context.buildResponseSend(abstractResponse)
      }
    }

    def responseNode(response: AbstractResponse): Option[JsonNode] = {
      if (RequestChannel.isRequestLoggingEnabled)
        Some(RequestConvertToJson.response(response, context.apiVersion))
      else
        None
    }

    def headerForLoggingOrThrottling(): RequestHeader = {
      envelope match {
        case Some(request) =>
          request.context.header
        case None =>
          context.header
      }
    }

    def requestDesc(details: Boolean): String = {
      val forwardDescription = envelope.map { request =>
        s"Forwarded request: ${request.context} "
      }.getOrElse("")
      s"$forwardDescription$header -- ${loggableRequest.toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new AlterConfigsRequest(newData, alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new IncrementalAlterConfigsRequest.Builder(newData).build(alterConfigs.version())

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def getConsumerFetchSizeBucketMetricName: Option[String] = {
      if (header.apiKey != ApiKeys.FETCH)
        None
      else {
        val isFromFollower = body[FetchRequest].isFromFollower
        val maxWait = body[FetchRequest].maxWait()
        // exclude the fetch requests that has fetch.max.wait.ms greater than the default setting for SizeBucketMetric.
        // Otherwise the P999 metrics do not reflect the broker performance correctly because P999 could be
        // just maxWait in the condition that there isn't sufficient data to immediately satisfy the requirement
        // given by fetch.min.bytes for some of the time and maxWait is set to a large number (e.g., 30 seconds)
        if (isFromFollower || maxWait > ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS) None
        else Some(metrics.getRequestSizeBucketMetricName(metrics.consumerFetchRequestSizeMetricNameMap, responseBytes))
      }
    }

    def getProduceAckSizeBucketMetricName: Option[String] = {
      if (header.apiKey != ApiKeys.PRODUCE)
        None
      else {
        var acks = body[ProduceRequest].acks()
        if(!metrics.produceRequestAcksSizeMetricNameMap.contains(acks)) {
          error(s"metrics.produceRequestAcksSizeMetricNameMap does not contain key acks '${acks}', use -1 instead.")
          acks = -1
        }
        Some(metrics.getRequestSizeBucketMetricName(metrics.produceRequestAcksSizeMetricNameMap(acks), sizeOfBodyInBytes))
      }
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metadataMetricNames =
        if (header.apiKey() == ApiKeys.METADATA && body[MetadataRequest].isAllTopics) {
          Seq(RequestMetrics.MetadataAllTopics)
        } else Seq.empty

      val metricNames =  (fetchMetricNames ++ metadataMetricNames ++ getConsumerFetchSizeBucketMetricName.toSeq ++
        getProduceAckSizeBucketMetricName.toSeq) :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestRateAcrossVersionsInternal.mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.totalTimeBucketHist.update(totalTimeMs)
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.responseBytesHist.update(responseBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      if (header.apiKey() == ApiKeys.METADATA) {
        BrokerMetadataStats.outgoingBytesRate.mark(responseBytes)
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val desc = RequestConvertToJson.requestDescMetrics(header, requestLog, response.responseLog,
          context, session, isForwarded,
          totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
          apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
          responseSendTimeMs, temporaryMemoryBytes,
          messageConversionsTimeMs)
        requestLogger.debug("Completed request:" + desc.toString)
      }
    }

    def releaseBuffer(): Unit = {
      envelope match {
        case Some(request) =>
          request.releaseBuffer()
        case None =>
          if (buffer != null) {
            memoryPool.release(buffer)
            buffer = null
          }
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer, " +
      s"envelope=$envelope)"

  }

  sealed abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    def onComplete: Option[Send => Unit] = None
  }

  /** responseLogValue should only be defined if request logging is enabled */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val observer: Observer,
                     val metrics: RequestChannel.Metrics) extends KafkaMetricsGroup {
  import RequestChannel._
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()
  val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)

  // Set this to Long.Maxvalue so that KafkaHealthCheck will not shutdown broker if it
  // reads lastDequeueTimeMs before lastDequeueTimeMs is updated by any KafkaRequestHandler thread.
  @volatile var lastDequeueTimeMs = Long.MaxValue
  // This metric can help user select a suitable threshold for requestMaxLocalTimeMs so that broker can shutdown itself only when it
  // is stuck or too slow. A suggested value of requestMaxLocalTimeMs could be twice the 999'th percentile of the RequestDequeuePollIntervalMs.
  private val requestDequeuePollIntervalMs = newHistogram("RequestDequeuePollIntervalMs")

  newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString))
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString))
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(
    request: RequestChannel.Request,
    response: AbstractResponse,
    onComplete: Option[Send => Unit]
  ): Unit = {
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    observeRequestResponse(request, response)
    val responseSend = request.buildResponseSend(response)
    request.responseBytes = responseSend.size()
    sendResponse(new RequestChannel.SendResponse(
      request,
      responseSend,
      request.responseNode(response),
      onComplete
    ))
  }

  def sendNoOpResponse(request: RequestChannel.Request): Unit = {
    observeRequestResponse(request, null)
    sendResponse(new network.RequestChannel.NoOpResponse(request))
  }

  def startThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new RequestChannel.StartThrottlingResponse(request))
  }

  def endThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /** Send a response back to the socket server to be sent over the network */
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    if (isTraceEnabled) {
      val requestHeader = response.request.headerForLoggingOrThrottling()
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest = {
    val curTime = time.milliseconds
    requestDequeuePollIntervalMs.update(curTime - lastDequeueTimeMs)
    lastDequeueTimeMs = curTime
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)
  }

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest = {
    val curTime = time.milliseconds
    requestDequeuePollIntervalMs.update(curTime - lastDequeueTimeMs)
    lastDequeueTimeMs = curTime
    requestQueue.take()
  }

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

  private def observeRequestResponse(request: RequestChannel.Request, response: AbstractResponse): Unit = {
    try {
      observer.observe(request.context, request.body[AbstractRequest], response)
    } catch {
      case e: Exception => error(s"Observer failed to observe ${Observer.describeRequestAndResponse(request, response)}", e)
    }
  }

}

class RequestBreakdownMetrics(val name: String) extends KafkaMetricsGroup {
  val HistogramMetricType = "histogram"
  val DurationDataType = "duration"
  val tags = Map("request" -> name)
}

object LiCombinedControlRequestBreakdownMetrics {
  // The MBean type tag is determined from class name.
  // Since we want the MBean still have type=RequestBreakdownMetrics, use a delegate instead of inheritance.
  private val metrics = new RequestBreakdownMetrics(ApiKeys.LI_COMBINED_CONTROL.name)
  private val decomposedRequestDelayedTime = mutable.Map[ApiKeys, Histogram]()
  private val decomposedRequestLocalTime = mutable.Map[ApiKeys, Histogram]()

  val RequestDelayedTimeMs = "RequestDelayedTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  // The metric names are exposed for observability
  val DurationHistogramMetricNames: mutable.Set[String] = mutable.Set[String]()
  val ApiKeysForDecomposedMetrics = Set(
    ApiKeys.LEADER_AND_ISR,
    ApiKeys.UPDATE_METADATA,
    ApiKeys.STOP_REPLICA
  )

  // Pre-initialize request that we want breakdown time metrics
  ApiKeysForDecomposedMetrics.foreach {apiKey =>
    getOrCreateDecomposedRequestDelayedTimeHistogram(apiKey)
    getOrCreateDecomposedRequestLocalTimeHistogram(apiKey)
  }

  private def decomposedRequestDelayedTimeMsMetricName(apiKey: ApiKeys): String = s"decomposed-${apiKey.name}-${RequestDelayedTimeMs}"

  private def decomposedRequestLocalTimeMsMetricName(apiKey: ApiKeys): String = s"decomposed-${apiKey.name}-${LocalTimeMs}"

  private def getOrCreateDecomposedRequestDelayedTimeHistogram(apiKey: ApiKeys): Histogram = {
    decomposedRequestDelayedTime.applyOrElse(
      apiKey,
      (apiKey: ApiKeys) => {
        val decomposedDelayedTimeMetricName = decomposedRequestDelayedTimeMsMetricName(apiKey)
        DurationHistogramMetricNames.add(decomposedDelayedTimeMetricName)
        metrics.newHistogram(decomposedRequestDelayedTimeMsMetricName(apiKey), biased = true, metrics.tags)
      }
    )
  }

  private def getOrCreateDecomposedRequestLocalTimeHistogram(apiKey: ApiKeys): Histogram = {
    decomposedRequestLocalTime.applyOrElse(
      apiKey,
      (apiKey: ApiKeys) => {
        val decomposedLocalTimeMetricName = decomposedRequestLocalTimeMsMetricName(apiKey)
        DurationHistogramMetricNames.add(decomposedLocalTimeMetricName)
        metrics.newHistogram(decomposedRequestLocalTimeMsMetricName(apiKey), biased = true, metrics.tags)
      }
    )
  }

  def markDecomposedRequestDelayedTime(apiKey: ApiKeys, requestQueueTime: Double): Unit = {
    getOrCreateDecomposedRequestDelayedTimeHistogram(apiKey).update(Math.round(requestQueueTime))
  }

  def markDecomposedRequestLocalTime(apiKey: ApiKeys, requestLocalTime: Double): Unit = {
    getOrCreateDecomposedRequestLocalTimeHistogram(apiKey).update(Math.round(requestLocalTime))
  }
}


object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"
  val MetadataAllTopics = ApiKeys.METADATA.name + "AllTopics"

  val RequestsPerSec = "RequestsPerSec"
  val RequestsPerSecAcrossVersions = "RequestsPerSecAcrossVersions"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val ResponseBytes = "ResponseBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"

  // generate map[bucketLeftBoundary, object] for buckets
  def getBucketBoundaryObjectMap[T](buckets: Array[Int], namePrefix: String, namePostfix: String,
    createValueFunc: String => T, addBinNum: Boolean): util.TreeMap[Int, T] = {
    // get bin boundary and corresponding name to be used in createValueFunc. (addBinNum would help with metrics ordering)
    // [0,1,10,50,100], prefix=Produce, postfix=Mb =>
    // leftBoundaryBucketNames:Seq((0, "Produce0To1Mb"), (1, "Produce1To10Mb"), (10, "Produce10To50Mb"),
    // (50, "Produce50To100Mb"),(100, "Produce100MbGreater"))
    val leftBoundaryBucketNames = for (i <- 0 until buckets.length) yield {
      val binNum = if (addBinNum) s"_Bin${i + 1}_" else ""
      val bucket = buckets(i)
      if (i == buckets.length - 1) (bucket, s"${namePrefix}${binNum}${bucket}${namePostfix}Greater")
      else (bucket, s"${namePrefix}${binNum}${bucket}To${buckets(i + 1)}${namePostfix}")
    }
    val treeMap = new util.TreeMap[Int, T]
    leftBoundaryBucketNames.foreach{case (boundary, name) => treeMap.put(boundary, createValueFunc(name))}
    treeMap
  }

  // get object for a given bucket boundary. the bucket is [left, right)
  def getBucketObject[T](bucketBoundaryObjectMap: util.TreeMap[Int, T], boundary: Int): T = {
    if (boundary < bucketBoundaryObjectMap.firstKey()) bucketBoundaryObjectMap.firstEntry().getValue
    else bucketBoundaryObjectMap.floorEntry(boundary).getValue
  }

}

class RequestMetrics(name: String, config: KafkaConfig) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // Compared with the requestRateInterval, the requestRateAcrossVersionsInternal is the request rate across all versions
  val requestRateAcrossVersionsInternal = newMeter(RequestsPerSecAcrossVersions, "requests", TimeUnit.SECONDS, tags)
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // response size in bytes
  val responseBytesHist = newHistogram(ResponseBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  // metrics to count the number of requests in different total time buckets.
  val totalTimeBucketHist = new Histogram("TotalTime", "Ms", config.requestMetricsTotalTimeBuckets)

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  class Histogram(val metricNamePrefix: String, val metricNamePostfix: String, val binBoundaries: Array[Int]) {
    // bin boundary with Int type should be good for current usages
    // If we need boundary bigger than Int.MaxValue, which probably indicates we should use a different unit

    // binLeftBoundary => (name, Counter)
    val boundaryCounterMap = createHistogram()

    def update(value: Double): Unit = {
      val valueLong = Math.round(value)
      update(valueLong)
    }

    def update(value: Long): Unit = {
      val valueInt = if(value > Int.MaxValue) Int.MaxValue else value.toInt
      update(valueInt)
    }

    def update(value: Int): Unit = {
      val counter = getBucketObject(boundaryCounterMap, value)._2
      counter.inc()
    }

    def removeHistogram(): Unit = {
      boundaryCounterMap.values.forEach(nameCounter => removeMetric(nameCounter._1, tags))
    }

    // [0,10,50], metricNamePrefix="TotalTime", metricNamePostfix="Mb" =>
    // Map(0=>("TotalTime0To10Ms", counter), 10=>("TotalTime10To50Ms", counter), 50=>("TotalTime50MsGreater", counter))
    private def createHistogram(): util.TreeMap[Int, (String, Counter)] = {
      getBucketBoundaryObjectMap(binBoundaries, metricNamePrefix, metricNamePostfix,
        name => (name, newCounter(name, tags)), true)
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestsPerSecAcrossVersions, tags)
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
    totalTimeBucketHist.removeHistogram()
  }
}
