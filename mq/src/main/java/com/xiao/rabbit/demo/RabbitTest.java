package com.xiao.rabbit.demo;

import com.rabbitmq.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitTest {
    private String EXCHANGE_NAME = "exchange_demo";
    private String ROUTING_KEY = "routingkey_demo";
    private String QUEUE_NAME = "queue_demo";
    private String IP_ADDRESS = "localhost";
    private int PORT = 5672;
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    @Before
    public void before() throws IOException, TimeoutException {
        factory = new ConnectionFactory();
        factory.setHost(IP_ADDRESS);
        factory.setPort(PORT);
        factory.setUsername("root");
        factory.setPassword("root");
        connection = this.factory.newConnection();
        channel = this.connection.createChannel();
    }


    /**
     * 生产一条消息
     *
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        //此处的ROUTING_KEY实际上是bindingKey
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        String message = "Hello World";
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
    }

    /**
     * 消费一条消息
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test2() throws IOException, InterruptedException {
        channel.basicQos(64);//设置客户端最多未被ack的消息数量
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("recv message: " + new String(body));
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        channel.basicConsume(QUEUE_NAME, consumer);
        TimeUnit.SECONDS.sleep(5);
    }


    /**
     * consume方式接收消息
     * @throws IOException
     */
    @Test
    public void test3() throws IOException {
        channel.basicQos(64);
        //不自动确认接收到消息
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, "myConsumTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String routingKey = envelope.getRoutingKey();
                String contentType = properties.getContentType();
                long deliveryTag = envelope.getDeliveryTag();
                //显示确认接收消息
                channel.basicAck(deliveryTag, false);
            }
        });
    }

    /**
     * get方式消费
     */
    @Test
    public void test4() throws IOException {
        GetResponse response = channel.basicGet(QUEUE_NAME, false);
        System.out.println(new String(response.getBody()));
        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
    }

    /**
     * 添加监听的生产者
     *
     * @throws IOException
     */
    @Test
    public void test5() throws IOException {
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        //此处的ROUTING_KEY实际上是bindingKey
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        String message = "Hello World";
        channel.basicPublish(EXCHANGE_NAME, "abc", true, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        //监听被mq退回的消息
        channel.addReturnListener((int i, String s, String s1, String s2, AMQP.BasicProperties basicProperties, byte[] bytes) -> {
            String msg = new String(bytes);
            System.out.println("Basic.return的返回结果是： " + msg);
        });
    }

    /**
     * 备胎交换器
     */
    @Test
    public void test6() throws IOException {
        HashMap<String, Object> args = new HashMap<>();
        args.put("alternate-exchange", "myAe");
        channel.exchangeDeclare("normalExchange", "direct", true, false, args);
        channel.exchangeDeclare("myAe", "fanout", true, false, null);
        channel.queueDeclare("normalQueue", true, false, false, null);
        channel.queueBind("normalQueue", "normalExchange", "normalKey");
        channel.queueDeclare("unroutedQueue", true, false, false, null);
        channel.queueBind("unroutedQueue", "myAe", "AeKey");
        //这条消息被正常路由到normalQueue
        channel.basicPublish("normalExchange", "normalKey", false, MessageProperties.PERSISTENT_TEXT_PLAIN, "hello".getBytes());
        //这条消息被转移到unrouteQueue,mandatory设置为true也会被路由到备份交换器
        channel.basicPublish("normalExchange", "testKey", false, MessageProperties.PERSISTENT_TEXT_PLAIN, "hello".getBytes());
    }

    /**
     * 事务
     */
    @Test
    public void test7() throws IOException {
        channel.txSelect();
        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, "transction message".getBytes());
        channel.txCommit();
    }

    /**
     * 生产这的发送，确认模式
     */
    @Test
    public void test8() throws IOException, InterruptedException {
        channel.confirmSelect();
        channel.basicPublish("exchange", "routingKey", null, "publish confirm test".getBytes());
        if (channel.waitForConfirms()) {
            System.out.println("发送失败");
        }
    }

    @After
    public void after() throws IOException, TimeoutException {
        channel.close();
        connection.close();
    }
}
