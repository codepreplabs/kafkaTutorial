package com.codeprep.consumer.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroupsDemo {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupsDemo.class);
	
	public static void main(String[] args) {
		
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "first-app";
		String topic = "first_topic";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//create consumer
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
			
			//subscribe to topic
			consumer.subscribe(Arrays.asList(topic));
			
			//poll for new data
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				if(records != null) {
					for(ConsumerRecord<String, String> record : records) {
						if(record != null) {
							LOG.info("Key: {}, Value: {}, Partition: {}, offsets: {}", record.key(), record.value(), record.partition(), record.offset());
						}
					}
				}
			}
		}catch (Exception e) {
			LOG.error(e.getMessage());
		}
		
	}
}
