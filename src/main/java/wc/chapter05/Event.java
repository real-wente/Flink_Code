package wc.chapter05;

import java.sql.Timestamp;

/**
 * @author wentao
 * @date 2022-04-05  17:01
 */

public class Event {
    public String user;
    public String url;
    public Long timestamp;//时间戳

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    //重写toString方法
    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
        //创建一个时间戳把对应的时间戳传进去
    }



}
