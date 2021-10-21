package com.hf.guigu.state;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class StateTest1_OPeratorState {
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

        SingleOutputStreamOperator<Integer> mapStream = dataStream.map(new MyCountFunction());
        mapStream.print();
        env.execute();

    }
    public static class MyCountFunction implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer> {

        private Integer count = 0;
        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }


        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num: state){
                count+=num;
            }

        }
    }
}
