package com.hf.guigu.state;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.Count;
import org.apache.flink.util.Collector;

public class StateTest2_KeyedState_ValueState {
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

        dataStream.keyBy("id").flatMap(new CountAvgWithValueState()).print();


        env.execute();
    }

    public static class CountAvgWithValueState extends RichFlatMapFunction<SensorReading, Tuple2<String,Double>>{

        private ValueState<Tuple2<Long,Double>> countAndSum;



        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //注册状态
            ValueStateDescriptor<Tuple2<Long, Double>> descriptor = new ValueStateDescriptor<Tuple2<Long, Double>>(
                    "average", //状态名称
                    Types.<Tuple2<Long, Double>>TUPLE(Types.LONG, Types.DOUBLE));//状态存储类型

            countAndSum = getRuntimeContext().getState(descriptor);
        }



        /**
         * 每来一条数据，都会调用此方法，key相同
         * @param value
         * @param out
         * @throws Exception
         */
        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<String, Double>> out) throws Exception {
            //拿到当前key的状态
            Tuple2<Long,Double> currentState = countAndSum.value();

            if (currentState==null){
                currentState = Tuple2.of(0L,0.0);
            }

            currentState.f0 +=1;
            currentState.f1 +=value.getTemperature();
            //更新状态
            countAndSum.update(currentState);

            /*// 判断，如果当前的 key 没出现了 3 次，则需要计算平均值，并且输出
            if (currentState.f0 %3==0) {
                double avg = currentState.f1 / currentState.f0;
                // 输出 key 及其对应的平均值
                out.collect(Tuple2.of(value.getId(), avg));

            }*/
            out.collect(Tuple2.of(value.getId(),currentState.f1));


        }
        @Override
        public void close() throws Exception {
            super.close();
            //  清空状态值
            countAndSum.clear();
        }
    }
}
