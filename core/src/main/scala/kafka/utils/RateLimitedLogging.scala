package kafka.utils

import java.util.concurrent.ConcurrentHashMap

class RateLimitedLogging extends Logging{

  //for rate limited log
  private var rateLimitLogNextLogTimeMap: ConcurrentHashMap[String, Long] =  new ConcurrentHashMap()
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
}
