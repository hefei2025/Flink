package com.hf.guigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        //环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = Integer.parseInt(parameterTool.get("port"));
        //从文件中获取数据流
        //DataStream data = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\word.txt");

        //从socket中获取数据流
        DataStream data = env.socketTextStream(host,port);

        //算子
        DataStream<Tuple2<String,Integer>> count = data.flatMap(new FlatMapFunction<String,Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                String[] words = value.toLowerCase().split("\\W+");
                for (String word: words) {
                    if (word!=null && word.length()>0){
                        out.collect(new Tuple2<String, Integer>(word,1));
                    }
                }
            }
        }).keyBy(0).sum(1);
        
        //sink
        count.print();
        
        //执行
        env.execute();



    }
}
