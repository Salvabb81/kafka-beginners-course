package com.github.salva.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

		String bootstrapServers = "127.0.0.1:9092";

		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			StringBuilder topic = new StringBuilder("first_topic");
			StringBuilder value = new StringBuilder("Hello world ");
			StringBuilder key = new StringBuilder("id_");
			value.append(i);
			key.append(i);

			// Create a Producer Record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic.toString(), key.toString(),
					value.toString());

			logger.info("Key: " + key);

			// send data
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception ex) {
					// executes every time a record is successfully sent or an exception is thrown
					if (ex == null) {
						logger.info("Received new metada. \n" + "Topic:" + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "TimeStamp: "
								+ metadata.timestamp());
					} else {
						logger.error("Error while producing", ex);
					}

				}
			}).get(); // block the send() to make it synchronous - DON'T DO THIS IN PRODUCTION!!
		}

		// flush data
		producer.flush();

		// flush and close producer
		producer.close();

	}

}
