import java.util.*;
public class RunConsumer {
    public static String topic;
    public static void main(String[] args) {
    	topic = args[2];
        ConsumerGroupExample consumer = new ConsumerGroupExample(args[0], args[1], args[2]);
        consumer.run(1);
    }
}