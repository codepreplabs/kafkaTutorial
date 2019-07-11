package com.codeprep.consumer.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAssignSeekDemo {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class);
	
	public static void main(String[] args) {
		
		String bootStrapServers = "127.0.0.1:9092";
		String topic = "first_topic";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//create consumer
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)){
			
			//assign and seek are mostly used to replay data or fetch a specific message
			
			//assign
			TopicPartition topicPartition = new TopicPartition(topic, 0);
			consumer.assign(Arrays.asList(topicPartition));
			
			//seek
			consumer.seek(topicPartition, 15L);
			
			boolean isReadEnabled = true;
			int maxNumberOfReads = 5;
			int numberOfDataRead = 0;
			
			//poll for new data
			while(isReadEnabled) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				if(records != null) {
					for(ConsumerRecord<String, String> record : records) {
						if(record != null) {
							LOG.info("Key: {}, Value: {}, Partition: {}, offsets: {}", record.key(), record.value(), record.partition(), record.offset());
							numberOfDataRead++;
						}
						if(numberOfDataRead >= maxNumberOfReads) {
							isReadEnabled = false;
							break;
						}
					}
				}
			}
		}catch (Exception e) {
			LOG.error(e.getMessage());
		}
		
	}
}
