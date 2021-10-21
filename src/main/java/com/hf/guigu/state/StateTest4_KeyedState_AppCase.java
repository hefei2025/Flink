package com.hf.guigu.state;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import sun.management.Sensor;

/**
 * 前后温度差值大于某个值，提示预警
 */
public class StateTest4_KeyedState_AppCase {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据
        //DataStream<String> inputStream = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        dataStream.keyBy("id").flatMap(new TempWarningMap(10.0)).print();

        env.execute();
    }
    public static class TempWarningMap extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

        //阈值
        private Double threshold;

        public TempWarningMap(Double threshold){
            this.threshold = threshold;
        }

        //值状态定义
        private ValueState<Double> lastTemp;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Double> descriptor
                    = new ValueStateDescriptor<Double>("lastTemp", Double.class);
            lastTemp = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTempValue = lastTemp.value();
            if (lastTempValue != null){
                Double diff = Math.abs(value.getTemperature() - lastTempValue);
                if (diff >= threshold){
                    out.collect(new Tuple3<String, Double, Double>(value.getId(),value.getTemperature(),lastTempValue));
                }
            }
            lastTemp.update(value.getTemperature());
        }
    }
}
