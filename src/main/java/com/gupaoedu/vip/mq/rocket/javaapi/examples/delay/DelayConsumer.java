package com.gupaoedu.vip.mq.rocket.javaapi.examples.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DelayConsumer {
    public static void main(String[] args) throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my_test_consumer_group");

        // Specify name server addresses.
        consumer.setNamesrvAddr("192.168.8.144:9876;192.168.8.146:9876");
        consumer.setMessageModel(MessageModel.BROADCASTING);

        // Subscribe one more more topics to consume.
        consumer.subscribe("delay-topic", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

                for(MessageExt msg : msgs){
                    String topic = msg.getTopic();
                    String messageBody="";
                    try {
                        messageBody = new String(msg.getBody(),"utf-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        // ????????????
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    String tags = msg.getTags();
                    SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    System.out.println("???????????????" +sf.format(new Date()) + "???topic:"+topic+",tags:"+tags+",msg:"+messageBody);
                }

                // ????????????
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
