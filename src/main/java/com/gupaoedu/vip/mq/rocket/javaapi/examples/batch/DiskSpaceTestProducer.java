/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.gupaoedu.vip.mq.rocket.javaapi.examples.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

public class DiskSpaceTestProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroupName");
        producer.setNamesrvAddr("192.168.100.101:9876");
        producer.start();

        //If you just send messages of no more than 1MiB at a time, it is easy to use batch
        //Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
        String topic = "BatchTest";
        for(int i =0; i< 10000000; i++){

            Message msg = new Message("test-disk-topic",
                    "TagA",
                    "2673",
                    ("RocketMQj4lk3j2kl4j3hjjkhklhjiuhuittyftygiut67tybut67tgted4we65t68gtf678t697y7y8vtvt78ty78ygyugygyut786y7g7y8gyigfsdf435t43tr4t43543r435fdsfdsfdsf432fdsfdsfdsfds243edsdsadsadsadsadsadse213w21jkhkjhkl4j3 "+String.format("%05d", i)).getBytes());

            SendResult sendResult = producer.send(msg);

        }
        System.out.println("全部干完");

    }
}
