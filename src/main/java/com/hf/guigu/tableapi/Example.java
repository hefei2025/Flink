/*
package com.hf.guigu.tableapi;

import com.hf.guigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Example {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<String> inputStream = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\sensor.txt");
        //转换成SensorReading
        DataSet<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);


        //table api 查询数据
        Table dataTable = tableEnv.fromDataSet(dataStream);
        Table resultTable = dataTable.select("id,temperature").where("id='sensor_1'");
        //注册成表，使用SQL api 查询数据
        tableEnv.registerTable("sensor",dataTable);
        String SQL = "select id,temperature from sensor where id = 'sensor_1'";
        Table resultSql = tableEnv.sqlQuery(SQL);

        //打印结果
        DataSet<Result> tableResult = tableEnv.toDataSet(resultTable, Result.class);
        DataSet<Row> sqlResult = tableEnv.toDataSet(resultSql, Row.class);

        tableResult.print("table");
        sqlResult.print("sql");

        env.execute();

    }

    public static class Result {
        public String id;
        public Double temperature;

        public Result(){
            super();
        }
        public Result(String id , Double temperature){
            this.id = id;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return id+","+temperature;
        }
    }


}
*/
