package com.gupaoedu.vip.mq.rocket.springbootapi;

import com.gupaoedu.vip.mq.rocket.springbootapi.producer.MessageSender;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DemoApplicationTests {

    @Autowired
    private MessageSender sender;

    @Test
    public void syncSendTest(){

        sender.syncSend();
    }

    /**
     * 发送 可靠异步消息
     * 发送消息后，不等mq响应，接着发送下一个数据包。发送方通过设置回调接口接收服务器的响应，并可对响应结果进行处理。
     * 异步发送一般用于链路耗时较长，对于RT响应较为敏感的业务场景，例如用户上传视频后通过启动转码服务，转码完成后通推送转码结果。
     */
    @Test
    public void asyncSendTest() throws Exception{
        sender.asyncSend();
    }

    /**
     * 发送单向消息
     */
    @Test
    public void sendOneWayTest(){

        sender.sendOneWay();
    }


    /**
     * 发送单向的顺序消息
     */
    @Test
    public void sendOneWayOrderlyTest(){

        sender.sendOneWayOrderly();
    }


}
