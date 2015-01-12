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

package org.apache.hadoop.metrics2;

import com.google.common.base.Objects;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.util.Time.now;

/**
 * Immutable list for metrics
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsList implements MetricsInfo {
  private final MetricsInfo info;
  private final List<Long> values;
  private final int period = 5*1000;
  private long firstPeriodInsertion;
  /**
   * Construct the metrics list with name, description and initial value
   * @param info  of the list
   */
  public MetricsList(MetricsInfo info) {
    this.info = checkNotNull(info, "list info");
    this.values = new LinkedList<Long>();
    this.firstPeriodInsertion = now();
    this.values.add(1L);
  }

  public void incr() {
    long now = now();
    if ((now-firstPeriodInsertion)<period) {
      int index = values.size()-1;
      long l = values.get(index);
      l++;
      values.remove(index);
      values.add(l);
    }
    else {
      firstPeriodInsertion = now;
      values.add(1L);
    }
  }

  @Override public String name() {
    return info.name();
  }

  @Override public String description() {
    return info.description();
  }

  /**
   * @return the info object of the list
   */
  public MetricsInfo info() {
    return info;
  }

  /**
   * Get the values of the list
   * @return List of values
   */
  public List<String> getValues() {
    List<String> returnList = new LinkedList<String>();

    for (long l : values) {
      returnList.add(String.valueOf(l));
    }
    return returnList;
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof MetricsList) {
      final MetricsList other = (MetricsList) obj;
      return Objects.equal(info, other.info()) &&
             Objects.equal(values, other.getValues());
    }
    return false;
  }

  @Override public int hashCode() {
    return Objects.hashCode(info, values);
  }

  @Override public String toString() {
    Objects.ToStringHelper returnObject = Objects.toStringHelper(this);
    //returnObject.add("info", info);
    returnObject.add("test", "toString");
    for (String s : getValues()) {
      returnObject.add("value", s);
    }

    return returnObject.toString();
  }
}
