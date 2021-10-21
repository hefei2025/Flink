package com.hf.guigu.window;

import com.hf.guigu.beans.SensorReading;
import com.hf.guigu.sink.SinkTest3_JDBC;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowTest_TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据
//        DataStream<String> inputStrem = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        //开窗测试-时间窗口
        SingleOutputStreamOperator<Object> windowStream = dataStream.keyBy("id")
                //滚动时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //增量窗口模式
                .aggregate(new AggregateFunction<SensorReading, Integer, Object>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator+1;
                    }

                    @Override
                    public Object getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a+b;
                    }
                });
                //滑动时间窗口
                //.window(SlidingProcessingTimeWindows.of(Time.minutes(1),Time.seconds(10)));
                //会话窗口
                //.window(EventTimeSessionWindows.withGap(Time.seconds(30)));

        //windowStream.print();

        //全量窗口模式
        dataStream.keyBy("id")
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long windEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<String, Long, Integer>(id,windEnd,count));
                    }
                })//.print()
        ;


        //dataStream.print();

        //开窗测试-计数窗口
        dataStream.keyBy("id").countWindow(10,2)
        .aggregate(new AggregateFunction<SensorReading, Tuple2<Double,Integer>, Object>() {
            @Override
            public Tuple2<Double, Integer> createAccumulator() {
                return new Tuple2<Double, Integer>(0.0,0);
            }

            @Override
            public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                return new Tuple2<Double, Integer>(accumulator.f0+value.getTemperature(),accumulator.f1+1);
            }

            @Override
            public Object getResult(Tuple2<Double, Integer> accumulator) {
                return accumulator.f0 / accumulator.f1;
            }

            @Override
            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                return new Tuple2<Double, Integer>(a.f0+b.f0,a.f1+b.f1);
            }
        }).print();


        env.execute();
    }


}
