package com.xzh.capter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class CustomSource implements SourceFunction<String> {

    private Boolean flag=true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Random random = new Random();

        while (true){
            Thread.sleep(100L);
            sourceContext.collect("custom:"+random.nextInt(10));
        }
    }

    @Override
    public void cancel() {
        flag=false;
    }
}
