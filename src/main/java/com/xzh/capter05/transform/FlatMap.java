package com.xzh.capter05.transform;

import com.xzh.capter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("zhangsan", "./cart", 3000L)
        );
        stream.flatMap((Event e, Collector<String> c)->{
            if(e.user.equals("Bob")){
                c.collect(e.user);
            }else{
                c.collect(e.user);
                c.collect(e.url);
                c.collect(e.timestamp+"");
            }
        }).returns(new TypeHint<String>() {})

        .print();



        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}