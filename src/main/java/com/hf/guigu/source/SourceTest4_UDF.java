
package com.hf.guigu.source;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();
        env.execute();
    }


    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading>{

        //定义一个标志位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //定义一个随机发生器
            final Random random = new Random();
            //设置10个传感器的初始温度
            HashMap<String, Double> sensorMap = new HashMap<String, Double>();
            for (int i = 0; i < 10; i++) {
                sensorMap.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }


            while (running){
                for (String sensorId: sensorMap.keySet()) {
                    //在当前温度基础上随机波动
                    Double nextTemp = sensorMap.get(sensorId) + random.nextGaussian();
                    sensorMap.put(sensorId,nextTemp);
                    sourceContext.collect(new SensorReading(sensorId,System.currentTimeMillis(),nextTemp));
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false ;
        }
    }
}
