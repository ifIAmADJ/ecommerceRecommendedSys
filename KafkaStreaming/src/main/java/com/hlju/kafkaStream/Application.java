package com.hlju.kafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {

        String brokers = "hadoop100:9092";
        String zookeepers = "hadoop100:2181";

        // 定义输入的 topic

        String from = "log";

        // 定义输出的 topic
        // 这个 topic 是真正被在线分析系统消费的消息队列
        String to  =  "recommender";

        // 定义 kafka stream 配置参数
        Properties settings = new Properties();

        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        // 创建 kafka stream config 配置。
        StreamsConfig config = new StreamsConfig(settings);


        // 定义拓扑构建器
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE",from)
        .addProcessor("PROCESSOR",LogProcessor::new,"SOURCE")
        .addSink("SINK",to,"PROCESSOR");


        KafkaStreams streams = new KafkaStreams(
                builder,config
        );

        streams.start();
        System.out.println("kafka stream starting....");







    }
}
