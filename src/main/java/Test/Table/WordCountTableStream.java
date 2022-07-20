package Test.Table;

import Test.MySource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.*;

/**
 * @author wentao
 * @date 2022-04-14  14:22
 */

public class WordCountTableStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();


        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv,bsSettings);

        DataStream<String> dataStream = sEnv.addSource(new MySource());

        Table table1 = tEnv.fromDataStream(dataStream,$("word"));

        Table table = table1
                .where($("word")
                .like("%t%"));

        String explantiong_old = tEnv.explain(table);
        System.out.println(explantiong_old);

        tEnv.toAppendStream(table, Row.class)
                .print("table");

        sEnv.execute();

    }
}
