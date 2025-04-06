package filter;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消息生产者 通过SQL 过滤
 */
public class FilterBySQLProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("filter_by_sql_producer_group");
        producer.setNamesrvAddr("172.18.165.230:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hello RocketMQ " + i).getBytes();
            Message message = new Message("FilterBySQLTopic","myTag", body);
            message.putUserProperty("age", String.valueOf(i));
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
