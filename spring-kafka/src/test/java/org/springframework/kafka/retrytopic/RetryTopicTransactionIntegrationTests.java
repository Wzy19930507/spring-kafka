/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * {@link org.springframework.kafka.listener.KafkaMessageListenerContainer} container transaction
 * with {@link org.springframework.kafka.annotation.RetryableTopic} non-blocking retries.
 *
 * @author Wang Zhiyang
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = {
			RetryTopicTransactionIntegrationTests.NON_BLOCK_RETRY_TRANSACTION_FIRST_TOPIC,
			RetryTopicTransactionIntegrationTests.CONTAINER_TRANSACTION_WITH_NON_BLOCK_RETRY_FIRST,
		}, partitions = 1,
		brokerProperties = { "transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class RetryTopicTransactionIntegrationTests {

	public final static String NON_BLOCK_RETRY_TRANSACTION_FIRST_TOPIC = "nonBlockingRetryTransactionFirst";

	public final static String CONTAINER_TRANSACTION_WITH_NON_BLOCK_RETRY_FIRST = "containerTxnsWithNonBlockingRetry1";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	@DisplayName("container transactions with @Transactional support non-blocking retries")
	void shouldRetryableTopicWithKafkaListenerNestedKafkaTransactionsAndTransactional() {
		kafkaTemplate.executeInTransaction(t ->
				kafkaTemplate.send(NON_BLOCK_RETRY_TRANSACTION_FIRST_TOPIC, "Testing topic 2")
		);
		assertThat(awaitLatch(latchContainer.countDownLatchOneRetryable)).isTrue();
		ConsumerRecord<String, String> consumerRecord =
				kafkaTemplate.receive(CONTAINER_TRANSACTION_WITH_NON_BLOCK_RETRY_FIRST, 0, 4);
		assertThat(consumerRecord).isNotNull();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isFalse();
		assertThat(KafkaListenerWithRetryableAndTransactionalAnnotation.SEND_MESSAGE_COUNT).isGreaterThan(4);
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(10, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			fail(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Component
	static class KafkaListenerWithRetryableAndTransactionalAnnotation {

		@Autowired
		CountDownLatchContainer countDownLatchContainer;

		@Autowired
		KafkaTemplate<String, String> kafkaTemplate;

		static int SEND_MESSAGE_COUNT = 0;

		@RetryableTopic(topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
		@KafkaListener(topics = RetryTopicTransactionIntegrationTests.NON_BLOCK_RETRY_TRANSACTION_FIRST_TOPIC)
		@Transactional(value = "transactionManager", propagation = Propagation.REQUIRES_NEW)
		void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchOneRetryable.countDown();
			kafkaTemplate.send(CONTAINER_TRANSACTION_WITH_NON_BLOCK_RETRY_FIRST, "message-" + ++SEND_MESSAGE_COUNT);
		}

		@DltHandler
		void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchDltOne.countDown();
		}

	}

	@Component
	static class CountDownLatchContainer {
		CountDownLatch countDownLatchOneRetryable = new CountDownLatch(3);
		CountDownLatch countDownLatchDltOne = new CountDownLatch(1);

	}

	@EnableKafka
	@EnableTransactionManagement
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		KafkaListenerWithRetryableAndTransactionalAnnotation kafkaListenerWithRetryableAndTx() {
			return new KafkaListenerWithRetryableAndTransactionalAnnotation();
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory,
				KafkaTransactionManager<String, String> transactionManager) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			ContainerProperties props = factory.getContainerProperties();
			props.setPollTimeout(50L);
			KafkaTransactionManager<String, String> kafkaTransactionManager = spy(transactionManager);
			Mockito.doThrow(new RuntimeException()).when(kafkaTransactionManager).commit(any());
			props.setTransactionManager(kafkaTransactionManager);
			factory.setConcurrency(1);
			return factory;
		}

		@Bean
		ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
			configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(configProps);
			pf.setTransactionIdPrefix("listener.noRetries.");
			return pf;
		}

		@Bean
		public KafkaTransactionManager<String, String> transactionManager() {
			return new KafkaTransactionManager<>(producerFactory());
		}

		@Bean
		KafkaTemplate<String, String> kafkaTemplate() {
			KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setConsumerFactory(consumerFactory());
			return kafkaTemplate;
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			return new DefaultKafkaConsumerFactory<>(props);
		}

	}

}
