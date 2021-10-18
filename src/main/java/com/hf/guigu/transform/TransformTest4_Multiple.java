package com.hf.guigu.transform;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TransformTest4_Multiple {
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

        //1.分流,按照温度值35为界分成两条流
        final OutputTag<String> highOutputTag = new OutputTag<String>("high"){};
        final OutputTag<String> lowOutputTag = new OutputTag<String>("low"){};
        SingleOutputStreamOperator<SensorReading> highprocess = mapStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getTemperature() > 35) {
                    collector.collect(sensorReading);
                    context.output(highOutputTag, "high-"+sensorReading.toString());
                }
            }
        });

        SingleOutputStreamOperator<SensorReading> lowprocess = mapStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() <= 35) {
                    out.collect(value);
                    ctx.output(lowOutputTag, "low-" + value.toString());
                }
            }

        });

        DataStream<String> highDataStream = highprocess.getSideOutput(highOutputTag);
        DataStream<String> lowDataStream = lowprocess.getSideOutput(lowOutputTag);

        //highDataStream.print("high");
        //lowDataStream.print("low");

        //2 合流 将高温转换成二元组类型，与低温流连接合并之后，输出状态信息
        // connect 不同类型的流合流，但只能联合两个流
        DataStream<Tuple2<String,Double>> waringStream = highprocess.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<String, Double>(value.getId(),value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = waringStream.connect(lowprocess);

        SingleOutputStreamOperator<Object> map = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<String,Double,String>(value.f0,value.f1,"high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple3<String,Double,String>(value.getId(),value.getTemperature(),"normal");
            }
        });
        //map.print();


        //union 合流，可以联合多条流（可以大于两个），但是联合的多条流数据类型必须一致
        DataStream<SensorReading> union = highprocess.union(lowprocess);
        union.print();
        env.execute();

    }
}
