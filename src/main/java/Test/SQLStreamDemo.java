package Test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wentao
 * @date 2022-04-14  11:29
 */

public class SQLStreamDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv =StreamTableEnvironment.create(env);

        DataStream<String> stream = env.addSource(new MySource());

        Table table = tEnv.fromDataStream(stream,$("word"));

        Table result = tEnv.sqlQuery("select * from " + table + " where word like '%t%'");

        tEnv.toAppendStream(result, Row.class)
                .print();
        env.execute();
    }
}
