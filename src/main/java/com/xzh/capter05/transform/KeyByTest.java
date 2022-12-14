package com.xzh.capter05.transform;

import com.xzh.capter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Bob", "./cart2000", 2000L),
                new Event("Bob", "./cart2001", 2001L),
                new Event("Bob", "./cart2002", 2002L),
                new Event("zhangsan", "./cart0", 3000L),
                new Event("zhangsan", "./cart1", 30001L),
                new Event("zhangsan", "./cart2", 3002L),
                new Event("zhangsan", "./cart3", 30003L)
        );
        KeyedStream<Event, String> eventStringKeyedStream = stream.keyBy((Event e) -> {
            return e.user;
        });

        eventStringKeyedStream
        .max("timestamp")
        .print("max");
        eventStringKeyedStream
        .maxBy("timestamp")
        .print("maxby");


//        eventStringKeyedStream.

        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}