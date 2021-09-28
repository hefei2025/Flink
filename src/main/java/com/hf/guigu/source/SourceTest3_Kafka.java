package com.hf.guigu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka中读取数据
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //kafka配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.30.52:9092");
        /*properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");*/

        DataStream<String> dataStream
                = env.addSource(new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties));

        //打印输出
        dataStream.print();
        //执行
        env.execute();

    }
}
