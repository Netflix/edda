package com.netflix.edda

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.Executors
import concurrent.ExecutionContext
import com.netflix.servo.monitor.Monitors
import com.netflix.servo.DefaultMonitorRegistry

object ThreadPools {
  // 200 for thread parity with tomcat
  val queryPool = Executors.newFixedThreadPool(200)
  DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor("edda.threadpool.query", queryPool.asInstanceOf[ThreadPoolExecutor]))
  
  val observerPool = Executors.newFixedThreadPool(5)
  DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor("edda.threadpool.observer", observerPool.asInstanceOf[ThreadPoolExecutor]))

  var purgePool = Executors.newFixedThreadPool(1)
  DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor("edda.threadpool.purge", purgePool.asInstanceOf[ThreadPoolExecutor]))

  val electorPool = Executors.newFixedThreadPool(10)
  DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor("edda.threadpool.elector", electorPool.asInstanceOf[ThreadPoolExecutor]))
}

object QueryExecutionContext {
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutorService(ThreadPools.queryPool)
}

object ObserverExecutionContext {
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutorService(ThreadPools.observerPool)
}

object PurgeExecutionContext {
  implicit var ec: ExecutionContext = ExecutionContext.fromExecutorService(ThreadPools.purgePool)
}

object ElectorExecutionContext {
  implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutorService(ThreadPools.electorPool)
}

