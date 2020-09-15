Flink Metric Reporter over Kafka
----------------------------

Compile the source code and put the output jar to flink/plugins/metrics-kafka folder. Then modify flink/config/flink-conf.yaml as below:

```yaml
metrics.reporter.kafka.factory.class: org.apache.flink.metrics.kafka.KafkaReporterFactory
metrics.reporter.kafka.bootstrapServers: 192.168.7.202:9092
metrics.reporter.kafka.topic: flink-metrics
metrics.reporter.kafka.chunkSize: 20
metrics.reporter.kafka.filter: none
metrics.reporter.kafka.interval: 15 SECONDS
``` 