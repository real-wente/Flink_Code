package wc.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.util.Collector;

/**
 * @author wentao
 * @date 2022-04-12  15:18
 */

/**
 * 批处理有界数据流
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //加载或者创建数据源
        DataSet<String> text=env.fromElements(
          "Flink batch demo",
          "batch demo",
          "demo");

        /**
         * 转换数据
         * 把String类型转化为Tuple2<String,Integer>类型
         *
         * */
        DataSet<Tuple2<String,Integer>> ds=text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        ds.print();

    }

    static class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            /**
             * 按空格分词
             * collect输出得到的结果到新的类型，记录要收集的数据
             * value->out
             * */
            /**
             * 增强for循环，等价于
             for(int i = 0;i < s.length(); i++){
             String str = s[i]; //当成数组的写法
             }
             * */
            String[] words=value.split(" ");
            for (String word : words){
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
