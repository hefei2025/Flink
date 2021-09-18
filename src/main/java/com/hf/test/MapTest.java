package com.hf.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 元素处理
 * env: 批
 *
 * Source：fromElements
 *
 * Sink：print
 */
public class MapTest {
    public static void main(String[] args) throws Exception {
        //env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //source
        DataSet<Integer> dataSet = env.fromElements(1, 2, -3, 0, 5, -9, 8);
        //sink
        DataSet<Integer> dataSet2 = dataSet.map(new Tokenizer());

        //execute
        dataSet2.print();


    }

    //transformaction
    public static class Tokenizer implements MapFunction<Integer,Integer>{

        @Override
        public Integer map(Integer value) throws Exception {
            if (value>0){
                return 2*value;
            }else{
                return 0;
            }
        }
    }
}
