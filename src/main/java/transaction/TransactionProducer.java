package transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.*;

/**
 * 事物消息生产者
 */
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException {
        // 创建事务生产者实例
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer-group");

        // 设置NameServer地址
        producer.setNamesrvAddr("localhost:9876");

        // 定义线程池用于事务回查
        ExecutorService executorService = new ThreadPoolExecutor(
                2, 5, 100, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("transaction-check-thread");
                return thread;
            }
        });
        producer.setExecutorService(executorService);

        // 设置事务监听器
        producer.setTransactionListener(new ICBCTransactionListener());

        // 启动生产者
        producer.start();

        // 创建消息
        String[] tags = new String[]{"TAGA", "TAGB", "TAGC"};
        for (int i = 0; i < 3; i++) {
            byte[] body = ("Hello RocketMQ " + i).getBytes();
            Message message = new Message("TransactionTopic", tags[i], body);
            // 发送事务消息，第二个参数是执行本地事物需要的参数
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.printf("发送结果: %s%n", sendResult);
        }
    }
}
