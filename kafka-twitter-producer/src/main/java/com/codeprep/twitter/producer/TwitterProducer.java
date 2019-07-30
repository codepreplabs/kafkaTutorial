package com.codeprep.twitter.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TwitterProducer {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

	public static void main(String[] args) {

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

		// create a twitter client
		Client hosebirdClient = createTwitterClient(msgQueue);
		hosebirdClient.connect();

		// create a kafka producer
		KafkaProducer<String, String> producer = getKafkaProducer();
		
		// adding a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOG.info("{}", "Stopping application ...");
			hosebirdClient.stop();
			LOG.info("{}", "closing produces");
			producer.close();
			LOG.info("{}", "app successfully closed!");
		}));
		
		//loop to send tweets to kafka
		// on a different thread, or multiple different threads...
		
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				 msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				hosebirdClient.stop();
				LOG.error(e.getMessage());
			}
			if(msg != null) {
				LOG.info("msg recieved: {}", msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
					
					public void onCompletion(RecordMetadata arg0, Exception e) {
						
						if(e != null) {
							LOG.error(e.getMessage());
						}
					}
				});
			}
		}
	}

	public static Client createTwitterClient(BlockingQueue<String> msgQueue) {

		String consumerKey = "";
		String consumerSecret = "";
		String token = "";
		String secret = "";

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("india");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}
	
	public static KafkaProducer<String, String> getKafkaProducer(){
		
		String bootstrapServers = "127.0.0.1:9092";
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//enable idempotence
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		//settings for high throughput
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		return new KafkaProducer<>(properties);
	}
}
