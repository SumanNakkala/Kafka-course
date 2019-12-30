package tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer(){}

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        // create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // create a kafka Producer

        KafkaProducer producer = createKafkaProducer();

        // loop to send the tweets to Kafka

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg!=null){

                logger.info(msg);
                producer.send(new ProducerRecord("twitter-tweets",null,msg));

            }

        }

    }

    private KafkaProducer createKafkaProducer() {

        String bootstrapServers = "127.0.0.1:9092";

        String topic = "twitter-tweets";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        return  producer;


    }

    String consumerKey="wfLK0E32QG18dzE78WDfaDVCd";
    String consumerSecret="3OZ9wMP17RWEmIKb2SfGcAHGGS6gYCj7iAgnYiu6bHAnvSsjGL";
    String token="101798389-d3UcezHv1WLx2tSlAjHjsR2uNNKBdCgCPJSQglZC";
    String secret ="gf8vlDOE3Ry690PlIEbbIuWAyG7BkmtCeTIq2r403phlp";

    public  Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

        List<String> terms = Lists.newArrayList("Cricket");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth =
                new OAuth1(consumerKey, consumerSecret,token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;





    }
}
