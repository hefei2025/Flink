package com.hf.test.flinkSql;


import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;


public class FlinkSqlTest {
    public static void main(String[] args) throws Exception {
        //1 创建上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        //2 读取score.csv 文件，并且作为source输入
        DataSet<String> input = env.readTextFile("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\score.csv");

        DataSet<PlayerData> topInput = input.map(new MapFunction<String,PlayerData>() {
            @Override
            public PlayerData map(String value) throws Exception {
                String[] s = value.toString().split(",");
                return new PlayerData(String.valueOf(s[0]),
                        String.valueOf(s[1]),
                        String.valueOf(s[2]),
                        Integer.valueOf(s[3]),
                        Double.valueOf(s[4]),
                        Double.valueOf(s[5]),
                        Double.valueOf(s[6]),
                        Double.valueOf(s[7]),
                        Double.valueOf(s[8])
                        ) ;
            }
        });

        //3 将source注册成表
        Table topScore = tableEnv.fromDataSet(topInput);
        tableEnv.registerTable("score",topScore);

        //4 核心处理逻辑 SQL 的编写
        Table queryResult = tableEnv.sqlQuery("select player, \n" +
                "count(season) as num \n" +
                "FROM score \n" +
                "GROUP BY player \n" +
                "ORDER BY num desc \n" +
                "LIMIT 3");

        //5 输出结果
        //1) 打印结果
        DataSet<Result> result = tableEnv.toDataSet(queryResult, Result.class);
        result.print();
        //2) 输出到文件
        TableSink sink = new CsvTableSink("C:\\GitHub\\2021\\Flink\\src\\main\\resources\\result.csv","|");
        String[] fileNames = {"name","num"};
        TypeInformation[] fileType = {Types.STRING(), Types.LONG()};
        tableEnv.registerTableSink("results",fileNames,fileType,sink);
        queryResult.insertInto("results");
        env.execute();




    }


    public static class PlayerData {

        /**
         * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
         */
        public String season;
        public String player;
        public String play_num;
        public Integer first_court;
        public Double time;
        public Double assists;
        public Double steals;
        public Double blocks;
        public Double scores;

        public PlayerData() {
            super();
        }

        public PlayerData(String season,
                          String player,
                          String play_num,
                          Integer first_court,
                          Double time,
                          Double assists,
                          Double steals,
                          Double blocks,
                          Double scores ) {
            this.season = season;
            this.player = player;
            this.play_num = play_num;
            this.first_court = first_court;
            this.time = time;
            this.assists = assists;
            this.steals = steals;
            this.blocks = blocks;
            this.scores = scores;
        }

    }


    public static class Result{
        public String player;
        public long num;

        public Result(){
            super();
        }

        public Result(String player,long num){
            this.player = player;
            this.num = num;
        }

        @Override
        public String toString(){
            return  player+":"+num;
        }
    }
}
