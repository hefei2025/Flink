package com.hf.guigu.transform;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.management.Sensor;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据
        DataStream<String> inputStrem = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");

        //转换成SensorReading
        DataStream<SensorReading> mapStream = inputStrem.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],new Long(fields[1]), new Double(fields[2]));
            }
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");
        //滚动聚合，去当前最大温度值
        //SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature");
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print();



        env.execute();

    }
}
