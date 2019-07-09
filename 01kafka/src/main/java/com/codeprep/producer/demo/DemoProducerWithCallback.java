package com.codeprep.producer.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoProducerWithCallback {

	private static Logger LOG = LoggerFactory.getLogger(DemoProducerWithCallback.class);

	public static void main(String[] args) {

		String bootstrapServers = "127.0.0.1:9092";

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		ProducerRecord<String, String> rec = new ProducerRecord<String, String>("first_topic", "Hello world!");
		producer.send(rec, new Callback() {

			public void onCompletion(RecordMetadata recordMeta, Exception exc) {

				if (exc == null) {
					LOG.info("Recieved new metadata \n Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
							recordMeta.topic(), recordMeta.partition(), recordMeta.offset(), recordMeta.timestamp());
				}else {
					exc.getStackTrace();
				}
			}
		});
		producer.close();
	}

}
