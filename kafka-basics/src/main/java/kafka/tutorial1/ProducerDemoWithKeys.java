package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

   static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

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
            String topic ="first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key :" + key);
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
            }).get(); // Don't do in the production env , this is only for learning purpose

        }

        producer.flush();
        producer.close();





    }
}
