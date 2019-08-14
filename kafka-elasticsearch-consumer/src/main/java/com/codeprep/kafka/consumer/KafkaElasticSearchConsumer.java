package com.codeprep.kafka.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaElasticSearchConsumer {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaElasticSearchConsumer.class);
	
	private static ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) {
		
		mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		try {
			RestHighLevelClient client = createClient();
			KafkaConsumer<String, String> consumer = createConsumer();
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				if(records != null) {
					LOG.info("Recieved: {} records", records.count());
					for(ConsumerRecord<String, String> record : records) {
						if(record != null) {
							
							IndexRequest indexRequest = new IndexRequest("twitter", "tweet").source(prepareAndGetJSON(record.value()), XContentType.JSON);
							IndexResponse response =  client.index(indexRequest, RequestOptions.DEFAULT);
							String id = response.getId();
							LOG.info("resp : {}", id);
							Thread.sleep(10);
						}
					}
					LOG.info("{}", "Committing offsets....");
					consumer.commitSync();
					LOG.info("{}", "Offsets have been committed");
					Thread.sleep(1000);
					
				}
			}
		} catch (JsonGenerationException | InterruptedException e) {
			LOG.error(e.getLocalizedMessage());
		} catch (IOException e) {
			LOG.error(e.getLocalizedMessage());
		}
	}
	
	private static String prepareAndGetJSON(String value) throws IOException {

		LOG.info("value: {}", value);
		if (value != null) {
			Tweet tweet = mapper.readValue(value, Tweet.class);
			return mapper.writeValueAsString(tweet);
		}
		return mapper.writeValueAsString(new Tweet());
	}

	public static KafkaConsumer<String, String> createConsumer(){
		
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "kafka-demo-consumer";
		String topic = "twitter_tweets";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	public static RestHighLevelClient createClient() {
		
		String hostname = "127.0.0.1";
		Integer port = 9200;
		
		return new RestHighLevelClient(
				  RestClient.builder(new HttpHost(hostname, port, "http")));
	}
}
