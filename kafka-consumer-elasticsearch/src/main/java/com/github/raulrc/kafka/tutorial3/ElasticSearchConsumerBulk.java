package com.github.raulrc.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerBulk {

    public static RestHighLevelClient createClient(){

        String hostname = "your_hostname";
        String username = "your_username";
        String password = "your_password";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient (HttpAsyncClientBuilder httpClientBuilder){
                            return httpClientBuilder.setDefaultCredentialsProvider
                                    (credentialsProvider);
                        }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerBulk.class.getName());

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        // poll for new data

        while (true) { // bad practise but for now it's ok
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record: records){
                String jsonString = record.value();
                // ID - 2 strategies
                // 1- kafka generic id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                // 2- Twitter feed specific id
                try{
                    String id = extractIdFromTweet(jsonString);
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // to make our consumer idempotent
                    ).source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest); // add to our bulk request
                } catch (NullPointerException e){
                    logger.warn("skipping bad data: " + record.value());
                }

            }
            if (recordCount > 0) {

                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Commiting the offsets...");
                consumer.commitSync();
                logger.info("Offsets have been commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //client.close();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";


        // Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;

    }
    private static JsonParser jsonParser = new JsonParser();

    public static String extractIdFromTweet(String tweetJson){
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

}
