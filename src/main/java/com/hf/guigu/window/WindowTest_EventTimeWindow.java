package com.hf.guigu.window;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowTest_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //定义事件时间，1.12以前默认为运行时间 (但1.12以后默认时间语义为事件时间)
 //       env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从socket中获取数据
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        SingleOutputStreamOperator<SensorReading> watermarkStream = dataStream
                //升序数据设置事件时间和watermark
                /* .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                     @Override
                     public long extractAscendingTimestamp(SensorReading element) {
                         return element.getTimestamp()*1000L;
                     }
                 } );*/
                //乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
                  /*.assignTimestampsAndWatermarks(new WatermarkStrategy<SensorReading>() {
                      @Override
                      public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                          return null;
                      }
                  });*/



        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};

                // 基于事件时间的开窗聚合，统计15秒内温度最小值
        SingleOutputStreamOperator<SensorReading> minTemp = dataStream
                //乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                })

                .keyBy("id")
                //.timeWindow(Time.seconds(15))
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTemp.print("minTemp");
        minTemp.getSideOutput(outputTag).print("late");

        env.execute();

    }
}
