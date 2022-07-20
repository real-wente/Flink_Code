package Test;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author wentao
 * @date 2022-04-12  16:55
 */
/**
 * 无界数据流随机产生，理论上永不停止
 * 自定义数据流，先继承SourceFunction接口
 * */
public class MySource implements SourceFunction<String> {

    private long count = 1L;
        private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning){

            List<String> stringList = new ArrayList<>();
            stringList.add("world");
            stringList.add("Flink");
            stringList.add("Stream");
            stringList.add("Batch");
            stringList.add("Table");
            stringList.add("SQL");
            stringList.add("hello");
            int size = stringList.size();
            int i = new Random().nextInt(size);
            ctx.collect(stringList.get(i));
            /**每秒产生一条数据*/
            Thread.sleep(1000);
        }
    }
    



    /**取消执行*/
    @Override
    public void cancel() {
        isRunning = false;
    }

}
