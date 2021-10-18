package com.hf.guigu.transform;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 复函数
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据
        DataStream<String> inputStrem = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");

        //转换成SensorReading
        DataStream<SensorReading> mapStream = inputStrem.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<String,Double,Integer>> resultStream = mapStream.map(new MyMapFunction());

        resultStream.print();

        env.execute();


    }

    public static class MyMapFunction extends RichMapFunction<SensorReading, Tuple3<String,Double,Integer>>{

        @Override
        public Tuple3<String,Double,Integer> map(SensorReading value) throws Exception {
            return new Tuple3<String,Double,Integer>(value.getId(),value.getTemperature(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
            super.open(parameters);

        }

        @Override
        public void close() throws Exception {
            //收尾工作，一般是清理状态或者关闭数据库连接
            System.out.println("close");
            super.close();
        }
    }
}
