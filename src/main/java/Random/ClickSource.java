package Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import wc.chapter05.Event;

import java.util.Calendar;
import java.util.Random;

/**
 * @author wentao
 * @date 2022-04-06  09:57
 */

public class ClickSource implements SourceFunction<Event> {
    //声明标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        //定义字段选择取的数据集
        String[] users={"Mary","Alice","Bob","Cary"};
        String[] urls={"Mary","Alice","Bob","Cary"};


        //循环不停的生成数据
        while (running){

            String user=users[random.nextInt(users.length)];
            String url=urls[random.nextInt(urls.length)];

            //生成时间戳
            Long timestamp= Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);

        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
