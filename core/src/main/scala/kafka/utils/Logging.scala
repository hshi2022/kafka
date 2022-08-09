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

package kafka.utils

import com.typesafe.scalalogging.Logger
import org.slf4j.{LoggerFactory, Marker, MarkerFactory}

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}


object Log4jControllerRegistration {
  private val logger = Logger(this.getClass.getName)

  try {
    val log4jController = Class.forName("kafka.utils.Log4jController").asInstanceOf[Class[Object]]
    val instance = log4jController.getDeclaredConstructor().newInstance()
    CoreUtils.registerMBean(instance, "kafka:type=kafka.Log4jController")
    logger.info("Registered kafka:type=kafka.Log4jController MBean")
  } catch {
    case _: Exception => logger.info("Couldn't register kafka:type=kafka.Log4jController MBean")
  }
}

private object Logging {
  private val FatalMarker: Marker = MarkerFactory.getMarker("FATAL")
}

trait Logging {

  protected lazy val logger = Logger(LoggerFactory.getLogger(loggerName))

  protected var logIdent: String = _

  Log4jControllerRegistration

  protected def loggerName: String = getClass.getName

  //for rate limited log
  private var rateLimitLogNextLogTimeMap: ConcurrentHashMap[String, Long] =  new ConcurrentHashMap()

  protected def msgWithLogIdent(msg: String): String =
    if (logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = logger.trace(msgWithLogIdent(msg))

  def trace(msg: => String, e: => Throwable): Unit = logger.trace(msgWithLogIdent(msg),e)

  def isDebugEnabled: Boolean = logger.underlying.isDebugEnabled

  def isTraceEnabled: Boolean = logger.underlying.isTraceEnabled

  def debug(msg: => String): Unit = logger.debug(msgWithLogIdent(msg))

  def debug(msg: => String, e: => Throwable): Unit = logger.debug(msgWithLogIdent(msg),e)

  def info(msg: => String): Unit = logger.info(msgWithLogIdent(msg))

  def info(msg: => String,e: => Throwable): Unit = logger.info(msgWithLogIdent(msg),e)

  // log at most once in intervalMs for the log tag
  def rateLimitedInfo(msg: => String, tag: => String, intervalMs: => Int): Unit = {
    val now = System.currentTimeMillis()
    if(!rateLimitLogNextLogTimeMap.containsKey(tag)) {
      rateLimitLogNextLogTimeMap.put(tag, Long.MinValue)
    }
    if (now > rateLimitLogNextLogTimeMap.get(tag)) {
      rateLimitLogNextLogTimeMap.put(tag, now + intervalMs)
      logger.info(s"RateLimitedLog for ${tag}: ${msgWithLogIdent(msg)} }")
    }
  }

  def warn(msg: => String): Unit = logger.warn(msgWithLogIdent(msg))

  def warn(msg: => String, e: => Throwable): Unit = logger.warn(msgWithLogIdent(msg),e)

  def error(msg: => String): Unit = logger.error(msgWithLogIdent(msg))

  def error(msg: => String, e: => Throwable): Unit = logger.error(msgWithLogIdent(msg),e)

  def fatal(msg: => String): Unit =
    logger.error(Logging.FatalMarker, msgWithLogIdent(msg))

  def fatal(msg: => String, e: => Throwable): Unit =
    logger.error(Logging.FatalMarker, msgWithLogIdent(msg), e)
}
