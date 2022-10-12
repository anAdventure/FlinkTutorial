package com.xzh.capter05.transform;

import com.xzh.capter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("zhangsan", "./cart", 3000L)
        );
         // 传入匿名类，实现 MapFunction
        SingleOutputStreamOperator<String> map = stream.map((MapFunction<Event, String>) e -> e.user);
//        map.filter()

        // 传入 MapFunction 的实现类
        stream.map(new UserExtractor()).print();

        stream.filter(s->{
            return !s.user.equals("Bob");
        }).print("filter:");



        env.execute();
    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e.user;
        }
    }
}