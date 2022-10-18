package com.xzh.capter05.transform;

import com.xzh.capter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {
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
                new Event("zhangsan", "./cart3", 30003L),
                new Event("zhangsan2", "./cart3", 30003L),
                new Event("zhangsan2", "./cart3", 30003L),
                new Event("zhangsan2", "./cart3", 30003L)
        );
     stream.map((Event e) -> {
            return Tuple2.of(e.user, 1L);
        })
         .returns(new TypeHint<Tuple2<String, Long>>() {})
         .keyBy(s->s.f0)
         .reduce((s1,s2)-> Tuple2.of(s1.f0,s1.f1+s2.f1))
         .returns(new TypeHint<Tuple2<String, Long>>() {})
         .keyBy(s->"aa")
          .reduce((s1,s2)-> s1.f1>s2.f1?s1:s2)

        .print();

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