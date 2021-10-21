package com.hf.guigu.state;

import com.hf.guigu.beans.SensorReading;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class StateTest3_KeyedState_ListState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");
        //DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        //转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        dataStream.keyBy("id").flatMap(new CountAvgWithListState());

        env.execute();
    }

    public static class CountAvgWithListState extends RichFlatMapFunction<SensorReading, Tuple2<String,Double>>{

        /**
         * valueState只能存一条数据
         * ListState 可以存多条数据
         */
        ListState<Tuple2<Long,Double>> countAndSum ;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Tuple2<Long, Double>> descriptor = new ListStateDescriptor<Tuple2<Long, Double>>("list-state",
                    Types.<Tuple2<Long, Double>>TUPLE(Types.LONG, Types.DOUBLE));
            countAndSum = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<String, Double>> out) throws Exception {
            //获取当前状态值
            Iterable<Tuple2<Long, Double>> currentState = countAndSum.get();
            if (currentState==null){
                countAndSum.addAll(Collections.<Tuple2<Long, Double>>emptyList());
            }
            countAndSum.add(new Tuple2<Long, Double>(1L,value.getTemperature()));

            List<Tuple2<Long, Double>> allElements = Lists.newArrayList((Iterator<? extends Tuple2<Long, Double>>) countAndSum.get());

            Long count = 0L;
            Double sum = 0.0;
            for (Tuple2<Long, Double> element:allElements){
                count++;
                sum+=element.f1;
            }
            out.collect(new Tuple2<String, Double>(value.getId(),sum));
        }

        @Override
        public void close() throws Exception {
            super.close();
            countAndSum.clear();
        }
    }
}
