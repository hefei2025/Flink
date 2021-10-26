package com.hf.guigu.state;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class StateTest6_FaultToLerance {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //状态后端配置
        // 1 MemoryStateBackend
        //env.setStateBackend(new MemoryStateBackend()); //已过时
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        // 2 FsStateBackend
        //env.setStateBackend(new FsStateBackend("hdfs://test")); //已过时
        /*env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://test");*/

        // 3 RocksDBStateBackend
        //env.setStateBackend(new RocksDBStateBackend() );

        //检查点配置
        env.enableCheckpointing(300);
        //检查点高级配置
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);//默认为false
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);//默认为0

        //重启策略
        //固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        //失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3
                , org.apache.flink.api.common.time.Time.minutes(10)
                , org.apache.flink.api.common.time.Time.minutes(2)));


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
