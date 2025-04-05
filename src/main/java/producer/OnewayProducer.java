package producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 单向消息发送生产者
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        /**
         * 使用RocketMQ生产者发送单向消息示例
         * 函数说明：
         * 1. 创建指定生产者组的生产者实例
         * 2. 配置NameServer地址
         * 3. 启动生产者实例
         * 4. 循环发送100条不等待响应的单向消息
         * 5. 最后关闭生产者释放资源
         * 注意：单向发送（sendOneway）不保证消息可靠性，适用于日志收集等可容忍丢失的场景
         */
        // 初始化生产者（生产者组名"pg"）
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        // 设置NameServer地址（消息队列协调服务地址）
        producer.setNamesrvAddr("192.168.0.104:9876");
        producer.start();
        // 批量发送100条测试消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            // 构造消息对象：主题singleTopic，标签someTag，消息体内容
            Message m = new Message("singleTopic", "someTag", body);
            
            // 使用单向模式发送消息（无等待/无回调的发送方式）
            producer.sendOneway(m);
        }
        // 关闭生产者并输出状态提示
        producer.shutdown();
        System.out.println("producer shutdown");
    }
}
