package com.hf.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 词频统计
 *
 * env: 批
 *
 * Source：readTextFile
 *
 * Sink：writeAsCsv
 *
 * 算子：Map
 */
public class WordCountTest {
    public static void main(String[] args) throws Exception {
        //env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //source
        DataSet<String> dataSet = env.readTextFile("C:\\GitHub\\Flink\\src\\main\\resources\\word.txt");

        //sink
        DataSet<Tuple2<String,Integer>> counts = dataSet.flatMap(new Tokenizer())
                .groupBy(0).sum(1);

        //execute
        String outPath = "C:\\GitHub\\Flink\\src\\main\\resources\\wordOut.txt";
        counts.writeAsText(outPath);
        env.execute("myFlink");
    }

    //transformscation
    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W");
            for (String token: tokens) {
                if (token.length()>0){
                    out.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }
}
