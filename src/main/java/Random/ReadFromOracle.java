package Random;

/**
 * @author wentao
 * @date 2022-04-07  16:18
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class ReadFromOracle {



    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Row> dataSource = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("oracle.jdbc.driver.OracleDriver")
                .setDBUrl("jdbc:oracle:thin:@172.20.254.14:1521:orcl")
                .setUsername("CQDX_JXGLXX")
                .setPassword("cquisse")
                .setQuery("select UNIQUEID,NAME from TE_ASSESSMENT_TYPE ")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO))
                .finish());


        dataSource.print();

        env.execute();
    }


    public static class Student {
        private Integer id;
        private String name;
    }
}