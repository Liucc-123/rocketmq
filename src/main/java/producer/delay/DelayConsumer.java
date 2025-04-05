package producer.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.client.exception.MQClientException;

import java.util.List;

/**
 * 延迟消息消费者
 */
public class DelayConsumer {
    public static void main(String[] args) throws MQClientException {
        // 定义一个 Push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        // 指定 nameserver
        consumer.setNamesrvAddr("localhost:9876");
        // 从第一条开始消费
        consumer.setConsumeFromWhere(org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅 DelayTopic，任意 tag
        consumer.subscribe("DelayTopic", "*");
        // 注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            /**
             * 一旦 broker 有消息来，这个方法会被调用；
             * @param msgs msgs.size() >= 1<br> DefaultMQPushConsumer.consumeMessageBatchMaxSize=1,you can modify here
             * @param context
             * @return
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    // 消费时间格式化
                    System.out.print(new java.text.SimpleDateFormat("mm:ss").format(new java.util.Date()));
                    System.out.println("Received message: " + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.println("DelayConsumer Started.");
    }
}