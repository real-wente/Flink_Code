package Random;

import Test.Table.MyOrder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wentao
 * @date 2022-04-15  09:29
 */

public class ReadSource {
    public static void main(String[] args) throws Exception {

        //流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStreamSource<Row> ORStreamSource = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("oracle.jdbc.driver.OracleDriver")
                .setDBUrl("jdbc:oracle:thin:@172.20.254.14:1521:orcl")
                .setUsername("CQDX_JXGLXX")
                .setPassword("cquisse")
                .setQuery("select UNIQUEID,NAME from TE_ASSESSMENT_TYPE ")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO))
                .finish()
        );

//        DataStream<Student> dataMap = ORStreamSource.flatMap(new MapFunction<Row,Student>(){
//            @Override
//            public Student map(Row value) throws Exception {
//                return new Student(
//                        (String) value.getField(0)
//                );
//            }
//        });
//    }
        //把数据流转换成表，暂时没得规则
        Table table1 = tEnv.fromDataStream(ORStreamSource);
        //DataStream<MyOrder> result = tEnv.toDataStream();
    }


    public static class Student {
        private Integer id;
        private String name;
    }


}
