package com.alibaba.middleware.race.rocketmq;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RocketMQConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;

public class MessagePushConsumer implements Serializable {
	private static final long serialVersionUID = 4641537253577312163L;

	private static final Logger LOG = LoggerFactory
			.getLogger(MessagePushConsumer.class);

	private final RocketMQConfig config;

	private transient DefaultMQPushConsumer consumer;

	public MessagePushConsumer(RocketMQConfig config) {
		this.config = config;
	}

	public void start(MessageListener listener) throws Exception {
		consumer = (DefaultMQPushConsumer) MessageConsumerManager
				.getConsumerInstance(config, listener, true);

		this.consumer.start();

		LOG.info("Init consumer successfully,configuration->{} !", config);
	}

	public void shutdown() {
		consumer.shutdown();

		LOG.info("Successfully shutdown consumer {} !", config);
	}

	public void suspend() {
		consumer.suspend();

		LOG.info("Pause consumer !");
	}

	public void resume() {
		consumer.resume();

		LOG.info("Resume consumer !");
	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer;
	}
}