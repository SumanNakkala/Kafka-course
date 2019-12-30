import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumer {


    public static RestHighLevelClient createClient(){

        //https://4wajwjstfo:6garwb2320@twitter-tweets-465715472.eu-west-1.bonsaisearch.net:443
        // replace with your own credentials
        String hostname = "twitter-tweets-465715472.eu-west-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "4wajwjstfo"; // needed only for bonsai
        String password = "6garwb2320"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {

        String jsonString = "{\"foo\" : \"bar\" }";

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        IndexRequest request = new IndexRequest("twitter","tweets").source(
                jsonString, XContentType.JSON
        );

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String id = response.getId();
        logger.info(id);

        client.close();




    }
}
