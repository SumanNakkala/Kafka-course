package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

   static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {

        System.out.println("Hello World");

        Properties properties = new Properties();

        // Create Producers Properties

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record

        for(int i =0;i<10;i++) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello World " + Integer.toString(i));


            //Send Data - Asynchronious

            producer.send(record, new Callback() {

                //executes on every successful record sent
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        logger.info("Recieved new metadata \n " + "Topic  :" + recordMetadata.topic() + "\n" +
                                "Partition    :" + recordMetadata.partition() + "\n" +
                                "Offset     :" + recordMetadata.offset() + "\n" +
                                "TimeStamp   :" + recordMetadata.timestamp());


                    } else {
                        logger.error("Error while producing messssage ", e);
                    }
                }
            });

        }

        producer.flush();
        producer.close();





    }
}
