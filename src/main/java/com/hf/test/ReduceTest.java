package com.hf.test;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 数据流汇总
 *
 * env: 流
 *
 * Source：addSource
 *
 * Sink：print
 *
 * 算子：keyBy、Reduce
 */
public class ReduceTest {

    private static final Logger log = LoggerFactory.getLogger(ReduceTest.class);
    private static String[] TYPE = {"苹果", "梨", "西瓜", "葡萄", "火龙果"};


    public static void main(String[] args) throws Exception {
        //evn
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        DataStreamSource<Tuple2<String,Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRuning = true ;
            private final Random random = new Random();
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
                while (isRuning){
                    Thread.sleep(1000);
                    sourceContext.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)],1));
                }
            }

            @Override
            public void cancel() {
                isRuning = false;
            }
        },"水果订单信息");

        //transformscation
        orderSource.keyBy(0)
                //将上一元素与当前元素相加后，返回给下一元素处理
                .reduce(new ReduceFunction<Tuple2<String,Integer>>(){
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        })
                //sink
                //.print()
        ;
        //sink
        PrintSinkFunction printSinkFunction = new PrintSinkFunction();
        orderSource.addSink(printSinkFunction).name("信息打印").setParallelism(2);

        //execute
        env.execute("数据流汇总");
    }
}
