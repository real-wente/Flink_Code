package wc.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wentao
 * @date 2022-06-21  16:13
 */
public class SourceTest {
    public static void main(String[] args) throws Exception{
         //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1. 从文件中你读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt ");
        //2.从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",1000L));
        //读取数据源
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        //3.从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1000L)
        );
        //4.从socket文本流中读取,不够稳定，吞吐量很小
        DataStreamSource<String> stream4 = env.socketTextStream("192.168.10.105", 7777);

        //5.从kafak读取数据

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.10.105:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> KafkaStream = env.addSource(new FlinkKafkaConsumer<String>
                ("clicks", new SimpleStringSchema(), properties));

        KafkaStream.print();
//        stream1.print("1");
//        numStream.print("num");
//        stream2.print("2");
//        stream3.print("3");
        //   stream4.print("4");

        //输出顺序需要考虑 还没解决这个问题
        env.execute();
    }
}
