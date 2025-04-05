package producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncProducer 类用于演示如何创建和使用 RocketMQ 的同步生产者
 */
public class SyncProducer {

    // 日志对象，用于记录日志信息
    private static final Logger log = LoggerFactory.getLogger(SyncProducer.class);

    /**
     * 主函数，执行同步消息生产者的创建和消息发送过程
     *
     * @param args 命令行参数
     * @throws MQClientException 当客户端发生异常时抛出
     * @throws MQBrokerException 当 Broker 发生异常时抛出
     * @throws RemotingException 当远程通信发生异常时抛出
     * @throws InterruptedException 当线程被中断时抛出
     */
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 创建一个默认的 MQ 生产者，并指定生产者组名为 "pg"
        DefaultMQProducer producer = new DefaultMQProducer("pg");

        // 设置 NameServer 的地址，NameServer 是 RocketMQ 的注册中心
        producer.setNamesrvAddr("192.168.0.104:9876");

        // 设置当发送消息失败时重试的次数
        producer.setRetryTimesWhenSendFailed(3);

        // 设置发送消息的超时时间
        producer.setSendMsgTimeout(5000);

        // 启动生产者
        producer.start();

        // 循环发送 100 条消息
        for (int i = 0; i < 100; i++) {
            // 消息体，包含简单的字符串信息
            byte[] body = ("Hi," + i).getBytes();

            // 创建一个消息对象，指定主题、标签和消息体
            Message msg = new Message("someTopic", "someTag", body);

            // 为消息设置键值，方便后续处理
            msg.setKeys("key-" + i);

            // 同步发送消息，并获取发送结果
            SendResult sendResult = producer.send(msg);

            // 记录发送结果日志
            log.info("sendResult:{}", sendResult);
        }

        // 关闭生产者
        producer.shutdown();
    }
}

