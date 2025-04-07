package consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费重试
 */
public class RetryConsumer {
    public static void main(String[] args) throws MQClientException {
        // 定义一个 Push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        // 指定 nameserver
        consumer.setNamesrvAddr("192.168.0.104:9876");
        // 从第一条开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅 someTopic，任意 tag
        consumer.subscribe("someTopic", "*");
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
                try {
                    // ...
                }catch (Throwable e){
                    // 以下三种情况均会引发消费重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                    return null;
//                    throw new RuntimeException("消费异常");
                }
                // 消费成功的返回结果
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.println("Consumer Started.");
    }
}
