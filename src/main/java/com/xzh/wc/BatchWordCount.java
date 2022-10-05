package com.xzh.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env =ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataSource<String> lineSource = env.readTextFile("input/words.txt");

        // 3.
        FlatMapOperator<String, Tuple2<String, Long>> operator = lineSource.flatMap((String s, Collector<Tuple2<String, Long>> collector) -> {

            for (String str : s.split(" ")) {
                collector.collect(Tuple2.of(str,1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> wcGroupBy = operator.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = wcGroupBy.sum(1);
        sum.print();


    }
}
