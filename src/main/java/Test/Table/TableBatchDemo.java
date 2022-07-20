package Test.Table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.xml.crypto.Data;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wentao
 * @date 2022-04-14  15:45
 */

/**
 *使用Table API 将DataStream转化为Table并过滤输出
 * 流批合一慢慢不使用DataSet API
 */
public class TableBatchDemo {
    public static void main(String[] args) throws Exception{
       // ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
/**两种定义方式二者等价*/
//        DataSet<MyOrder> input = env.fromElements(
//                new MyOrder(1L,"BMW",1),
//                new MyOrder(2L,"Tesla",8),
//                new MyOrder(2L,"TEsla",9),
//                new MyOrder(3L,"Roll-Royce",20));
//
//        DataSource<Object> input1 = env.fromElements(
//                new MyOrder(1L,"BMW",1),
//                new MyOrder(2L,"Tesla",8),
//                new MyOrder(2L,"TEsla",9),
//                new MyOrder(3L,"Roll-Royce",20)
//        );

        DataStreamSource<MyOrder> stream = env.fromElements(
                new MyOrder(1L,"BMW",1),
                new MyOrder(2L,"Tesla",8),
                new MyOrder(2L,"TEsla",9),
                new MyOrder(3L,"Roll-Royce",20)
        );
        Table table = tEnv.fromValues(stream);
        Table filtered = table.where($("amount")
                       .isGreaterOrEqual(8));
        /**
         * 把数据流转化为表
         *
         * */
        Table MyTable = tEnv.fromDataStream(stream);

        //DataSet<MyOrder> result = tEnv.toDataSet(filtered,MyOrder.class);


        DataStream<MyOrder> result = tEnv.toDataStream(filtered,MyOrder.class);
        result.print();
    }
}
