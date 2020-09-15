/*
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

package org.apache.flink.metrics.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstract reporter with registry for metrics.
 */
abstract class AbstractReporter implements MetricReporter {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Map<Gauge<?>, JSONObject> gauges = new HashMap<>();
    protected final Map<Counter, JSONObject> counters = new HashMap<>();
    protected final Map<Histogram, JSONObject> histograms = new HashMap<>();
    protected final Map<Meter, JSONObject> meters = new HashMap<>();
    protected final List<Metric> delayRemoveList = new ArrayList<>();

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        JSONObject metrics = convert(group);
        metrics.put("name", metricName);
        synchronized (this) {
            if (metric instanceof Counter) {
                counters.put((Counter) metric, metrics);
            } else if (metric instanceof Gauge) {
                gauges.put((Gauge<?>) metric, metrics);
            } else if (metric instanceof Histogram) {
                histograms.put((Histogram) metric, metrics);
            } else if (metric instanceof Meter) {
                meters.put((Meter) metric, metrics);
            } else {
                log.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
                        "does not support this metric type.", metric.getClass().getName());
            }
        }
    }

    private JSONObject convert(MetricGroup group) {
        JSONObject jsonObject = new JSONObject();
        for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
            String name = variable.getKey();
            jsonObject.put(name.substring(1, name.length() - 1), variable.getValue());
        }
        return jsonObject;
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            delayRemoveList.add(metric);
        }
    }

    protected void tryRemove() {
        List<Metric> removed = new ArrayList<>();
        for (Metric metric : delayRemoveList) {
            if (metric instanceof Counter) {
                counters.remove(metric);
            } else if (metric instanceof Gauge) {
                gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                histograms.remove(metric);
            } else if (metric instanceof Meter) {
                meters.remove(metric);
            } else {
                log.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
                        "does not support this metric type.", metric.getClass().getName());
            }
            removed.add(metric);
        }
        delayRemoveList.removeAll(removed);
    }
}
