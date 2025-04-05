package producer.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 延迟消息生产者
 */
public class DelayProducer {
    public static void main(String[] args) {
        try {
            // 实例化生产者组名
            DefaultMQProducer producer = new DefaultMQProducer("pg");
            // 设置NameServer地址
            producer.setNamesrvAddr("localhost:9876");
            producer.setSendMsgTimeout(60000);
            // 启动生产者实例
            producer.start();
            // 创建消息，并指定Topic、Tag和消息体
            for (int i = 0; i < 10; i++) {
                byte[] body = ("hi" + i).getBytes();
                Message msg = new Message("DelayTopic", "TagA", body);
                // 设置延时级别3,这个消息将在10秒后投递
                msg.setDelayTimeLevel(3);
                // 发送消息到一个Broker
                SendResult sendResult = producer.send(msg);
                // 输出消息被发送的时间
                System.out.print(new SimpleDateFormat("mm:ss").format(new Date()));
                // 通过sendResult返回消息是否成功送达
                System.out.printf("%s%n", sendResult);
            }

            // 如果不再发送消息，关闭生产者实例
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
