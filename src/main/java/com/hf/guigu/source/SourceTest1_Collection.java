package com.hf.guigu.source;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 从集合中读取数据
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_2", 1547718201L, 15.4),
                new SensorReading("sensor_3", 1547718202L, 6.7),
                new SensorReading("sensor_4", 1547718205L, 38.1)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 5, 6, 8, 9);

        //打印输出
        dataStream.print("data");
        integerDataStream.print("integer");

        //执行
        env.execute();
    }
}
