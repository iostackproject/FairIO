/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.lib;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.SampleStat;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * A mutable metric with stats.
 *
 * Useful for keeping throughput/latency stats.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableStatShare extends MutableMetric {
  private final MetricsInfo numInfo;
  private final MetricsInfo avgInfo;
  private final MetricsInfo stdevInfo;
  private final MetricsInfo iMinInfo;
  private final MetricsInfo iMaxInfo;
  private final MetricsInfo minInfo;
  private final MetricsInfo maxInfo;
  private final MetricsInfo initInfo;
  private final MetricsInfo endInfo;
  private final MetricsInfo intervalInfo;
  private final MetricsInfo bytesInfo;
  private final MetricsInfo sharedInfo;
  private final MetricsInfo weightInfo;
  private final MetricsInfo queuedInfo;

  private final SampleStat intervalStat = new SampleStat();
  private final SampleStat prevStat = new SampleStat();
  private final SampleStat.MinMax minMax = new SampleStat.MinMax();
  private long initTotalBytesProcessed;
  private MutableCounterLong totalBytesProcessed;

  private long numSamples = 0;
  private boolean extended = false;
  private float weight;
  private int queuedRequests;


  /**
   * Construct a sample statistics metric
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   * @param extended    create extended stats (stdev, min/max etc.) by default.
   */
  public MutableStatShare(String name, String description,
                          String sampleName, String valueName, boolean extended, MutableCounterLong bytes) {
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);
    numInfo = info(ucName +"Num"+ usName, "Number of "+ lsName +" for "+ desc);
    avgInfo = info(ucName +"Avg"+ uvName, "Average "+ lvName +" for "+ desc);
    stdevInfo = info(ucName +"Stdev"+ uvName,
                     "Standard deviation of "+ lvName +" for "+ desc);
    iMinInfo = info(ucName +"IMin"+ uvName,
                    "Interval min "+ lvName +" for "+ desc);
    iMaxInfo = info(ucName + "IMax"+ uvName,
                    "Interval max "+ lvName +" for "+ desc);
    minInfo = info(ucName +"Min"+ uvName, "Min "+ lvName +" for "+ desc);
    maxInfo = info(ucName +"Max"+ uvName, "Max "+ lvName +" for "+ desc);
    initInfo = info(ucName +"Init"+ uvName, "Init bytes "+ lvName +" for "+ desc);
    endInfo = info(ucName +"End"+ uvName, "Total bytes "+ lvName +" for "+ desc);
    intervalInfo = info(ucName +"Interval"+ uvName, "Total bytes "+ lvName +" for "+ desc);
    bytesInfo = info(ucName +"Class"+ uvName, "Total bytes "+ lvName +" for "+ desc);
    sharedInfo = info(ucName +"Shared"+ uvName, "Total bytes "+ lvName +" for "+ desc);
    weightInfo = info(ucName +"Weight"+ uvName, "Valor"+ lvName +" for "+ desc);
    queuedInfo = info(ucName +"Queued"+ "Requests", "num requests"+ lvName +" for "+ desc);
    this.queuedRequests = 0;
    this.totalBytesProcessed = bytes;
    this.initTotalBytesProcessed = bytes.value();
    this.extended = extended;
  }

  /**
   * Add a number of samples and their sum to the running stat
   * @param numSamples  number of samples
   * @param sum of the samples
   */
  public synchronized void add(long numSamples, long sum) {
    intervalStat.add(numSamples, sum);
    setChanged();
  }

  /**
   * Add a snapshot to the metric
   * @param value of the metric
   */
  public synchronized void add(long value) {
    intervalStat.add(value);
    minMax.add(value);
    setChanged();
  }

  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    long intervalBytesProcessed = totalBytesProcessed.value() - initTotalBytesProcessed;

    double classBytesProcessed = (intervalStat.mean()*(double)intervalStat.numSamples());
    double classShareProcessed = classBytesProcessed/intervalBytesProcessed;

    if (all || changed()) {
      numSamples += intervalStat.numSamples();
      builder.addCounter(numInfo, numSamples)
             .addGauge(weightInfo, weight)
             .addGauge(avgInfo, lastStat().mean())
             .addCounter(initInfo, initTotalBytesProcessed)
             .addCounter(endInfo, totalBytesProcessed.value())
             .addGauge(intervalInfo, intervalBytesProcessed)
             .addGauge(bytesInfo, classBytesProcessed)
             .addGauge(sharedInfo, classShareProcessed)
             .addCounter(queuedInfo, queuedRequests);

      if (extended) {
        builder.addGauge(stdevInfo, lastStat().stddev())
               .addGauge(iMinInfo, lastStat().min())
               .addGauge(iMaxInfo, lastStat().max())
               .addGauge(minInfo, minMax.min())
               .addGauge(maxInfo, minMax.max());
      }
      if (changed()) {
        if (numSamples > 0) {
          intervalStat.copyTo(prevStat);
          intervalStat.reset();
        }
        clearChanged();
      }
    }
    initTotalBytesProcessed = totalBytesProcessed.value();
  }

  private SampleStat lastStat() {
    return changed() ? intervalStat : prevStat;
  }

  /**
   * Reset the all time min max of the metric
   */
  public void resetMinMax() {
    minMax.reset();
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }

  public void setQueuedRequests(int queuedRequests) {
    this.queuedRequests = queuedRequests;
  }
}
