package com.xzh.capter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 一、数据源
        // 1、文件中读取数据
        DataStreamSource<String> fileSource = env.readTextFile("/Users/xiezihao/JavaProjects/BigData/Flink/FlinkTutorial/input/click.txt");
        // 2、从集合中读取数据
        DataStreamSource<String> collectionSource = env.fromCollection(Arrays.asList("1", "2", "3"));

        // 3、监听文本流数据，用于测试 nc -lk 7777
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 7777);
        fileSource.print();

        // 4、 从kafka中读取数据

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer<String>("clicks",new SimpleStringSchema(),properties));
        collectionSource.print("collectionSource");
        collectionSource.print();


       env.execute();

    }
}
