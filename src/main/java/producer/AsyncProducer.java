package producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
        // 异步发送失败不尽兴重试
        producer.setRetryTimesWhenSendFailed(0);
        // 指定创建 topic 的 queue 实例改为 2
        producer.setDefaultTopicQueueNums(2);
        // 启动Producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < 100; i++) {
            final int index = i;
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("myTopicA",
                    "myTagA",
                    "OrderID188",
                    body);
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }
                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        Thread.sleep(5000);
        producer.shutdown();
    }
}
