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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.stream.Collectors;


/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Kafka.
 */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter extends AbstractReporter implements Scheduled {
    private KafkaProducer<String, String> kafkaProducer;
    private List<String> metricsFilter = new ArrayList<>();
    private int chunkSize;
    private String topic;

    @Override
    public void open(MetricConfig metricConfig) {
        String bootstrapServer = metricConfig.getString("bootstrapServers", "localhost:9092");
        String filter = metricConfig.getString("filter", "numRecordsIn,numRecordsOut");
        String chunkSize = metricConfig.getString("chunkSize", "20");
        String topic = metricConfig.getString("topic", "flink-metrics");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("acks", "all");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        kafkaProducer = new KafkaProducer<>(properties);
        Thread.currentThread().setContextClassLoader(classLoader);
        if (!"none".equals(filter)) {
            this.metricsFilter.addAll(Arrays.asList(filter.split(",")));
        }
        this.chunkSize = Integer.parseInt(chunkSize);
        this.topic = topic;
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        if (this.metricsFilter.size() > 0 && this.metricsFilter.contains(metricName)) {
            super.notifyOfAddedMetric(metric, metricName, group);
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        if (this.metricsFilter.size() > 0 && this.metricsFilter.contains(metricName)) {
            super.notifyOfRemovedMetric(metric, metricName, group);
        }
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    @Override
    public void report() {
        synchronized (this) {
            tryReport();
            tryRemove();
        }
    }

    private void tryReport() {
        JSONArray jsonArray = new JSONArray();
        for (Gauge gauge : gauges.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("metric", gauges.get(gauge));
            jsonObject.put("name", gauges.get(gauge).getString("name"));
            jsonObject.put("value", gauge);
            jsonObject.put("type", "Gauge");
            jsonArray.add(jsonObject);
        }
        for (Counter counter : counters.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("metric", counters.get(counter));
            jsonObject.put("name", counters.get(counter).getString("name"));
            jsonObject.put("value", counter);
            jsonObject.put("type", "Counter");
            jsonArray.add(jsonObject);
        }
        for (Histogram histogram : histograms.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("metric", histograms.get(histogram));
            jsonObject.put("name", histograms.get(histogram).getString("name"));
            jsonObject.put("value", histogram);
            jsonObject.put("type", "Histogram");
            jsonArray.add(jsonObject);

        }
        for (Meter meter : meters.keySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("metric", meters.get(meter));
            jsonObject.put("name", meters.get(meter).getString("name"));
            jsonObject.put("value", meter);
            jsonObject.put("type", "Meter");
            jsonArray.add(jsonObject);

        }

        Map<String, List<Object>> values = jsonArray.stream().collect(Collectors.groupingBy((item) -> {
            JSONObject object = (JSONObject) item;
            return object.getString("name");
        }));

        for (String key : values.keySet()) {
            List<Object> objects = values.get(key);
            //spilt to chuck
            List<List<Object>> lists = new ArrayList<>();
            for (int i = 0; i < objects.size(); i += chunkSize) {
                int end = Math.min(objects.size(), i + chunkSize);
                lists.add(objects.subList(i, end));
            }
            for (List<Object> chunk : lists) {
                ProducerRecord<String, String> record = new ProducerRecord<>(this.topic, key, JSONObject.toJSONString(chunk));
                kafkaProducer.send(record);
            }

        }
    }
}
