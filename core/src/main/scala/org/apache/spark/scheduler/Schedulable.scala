/*
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

package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * :: DeveloperApi ::
 * A read-only view of the scheduling-relevant properties of a schedulable entity (a pool or a
 * task set). This is the public contract handed to a custom [[java.util.Comparator]] installed via
 * `SparkContext.setFairSchedulingComparator`, so that user code can decide the order of the fair
 * scheduler pools without depending on (or being able to mutate) Spark-internal scheduler types.
 */
@DeveloperApi
trait SchedulableInfo {
  /** The name of the schedulable (pool name). */
  def name: String
  /** The pool weight configured in the fair scheduler allocation file. */
  def weight: Int
  /** The minimum share configured for the pool. */
  def minShare: Int
  /** The number of currently running tasks in the pool. */
  def runningTasks: Int
  /** The priority, used to break ties (mainly relevant for FIFO). */
  def priority: Int
  /** The stage id, used to break ties (mainly relevant for FIFO). */
  def stageId: Int
}

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable extends SchedulableInfo {
  var parent: Pool
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  def schedulingMode: SchedulingMode
  def weight: Int
  def minShare: Int
  def runningTasks: Int
  def priority: Int
  def stageId: Int
  def name: String

  def isSchedulable: Boolean
  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  def executorDecommission(executorId: String): Unit
  def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}
