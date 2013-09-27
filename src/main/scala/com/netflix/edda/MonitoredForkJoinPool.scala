package com.netflix.edda

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.Callable

import com.netflix.servo.annotations.Monitor
import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge

class MonitoredForkJoinPool(pool: ForkJoinPool) {
  
  private[this] val activeCount: BasicGauge[java.lang.Integer] = new BasicGauge[java.lang.Integer](
    MonitorConfig.builder("activeCount").build(),
    new Callable[java.lang.Integer] {
      def call() = pool.getActiveThreadCount()
    })
  
  private[this] val corePoolSize: BasicGauge[java.lang.Integer] = new BasicGauge[java.lang.Integer](
    MonitorConfig.builder("corePoolSize").build(),
    new Callable[java.lang.Integer] {
      def call() = pool.getParallelism()
    })

  private[this] val poolSize: BasicGauge[java.lang.Integer] = new BasicGauge[java.lang.Integer](
    MonitorConfig.builder("poolSize").build(),
    new Callable[java.lang.Integer] {
      def call() = pool.getPoolSize()
    })

  private[this] val queueSize: BasicGauge[java.lang.Integer] = new BasicGauge[java.lang.Integer](
    MonitorConfig.builder("queueSize").build(),
    new Callable[java.lang.Integer] {
      def call() = pool.getQueuedSubmissionCount()
    })
    
  private[this] val queuedTaskSize: BasicGauge[java.lang.Long] = new BasicGauge[java.lang.Long](
    MonitorConfig.builder("queuedTaskSize").build(),
    new Callable[java.lang.Long] {
      def call() = pool.getQueuedTaskCount()
    })
}

