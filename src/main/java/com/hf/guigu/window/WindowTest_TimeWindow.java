package com.hf.guigu.window;

import com.hf.guigu.beans.SensorReading;
import com.hf.guigu.sink.SinkTest3_JDBC;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowTest_TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据
        DataStream<String> inputStrem = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");

        //转换成SensorReading
        DataStream<SensorReading> dataStream = inputStrem.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        //开窗测试
        WindowedStream<SensorReading, Tuple, TimeWindow> windowStream = dataStream.keyBy("id")
                //滚动时间窗口
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
                //滑动时间窗口
                //.window(SlidingProcessingTimeWindows.of(Time.minutes(1),Time.seconds(10)));
                //会话窗口
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)));
        dataStream.print();
        env.execute();
    }
}
