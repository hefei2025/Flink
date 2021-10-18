package com.hf.guigu.sink;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.postgresql.core.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest3_JDBC {

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
        dataStream.addSink(new MyJdbcSinkFunction());
        env.execute();

    }

    public static class MyJdbcSinkFunction extends RichSinkFunction<SensorReading>{
        Connection con = null;
        PreparedStatement insertstmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection("jdbc:postgresql://192.168.30.52:5432/relation","postgres","postgres");
            insertstmt = con.prepareStatement("insert into public.senserReading (id,temperature) values(?,?)");
            updateStmt = con.prepareStatement("update public.senserReading set temperature= ? where id = ?");

        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount()==0){
                insertstmt.setString(1,value.getId());
                insertstmt.setDouble(2,value.getTemperature());
                insertstmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            insertstmt.close();
            updateStmt.close();
            con.close();
        }
    }
}
