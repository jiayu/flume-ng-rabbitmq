/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink.rabbitmq;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeRabbitMQConstants;
import org.apache.flume.RabbitMQUtil;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RabbitMQSink extends AbstractSink implements Configurable {
	private static final Logger log = LoggerFactory.getLogger(RabbitMQSink.class);
	private CounterGroup counterGroup;
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private String queueName;
	private String exchangeName;
	private String routingKey;
	private int batchSize;
	private int confirmTimeout;

	public RabbitMQSink() {
		counterGroup = new CounterGroup();
	}

	//only used by unit test to overwrite the connection
	void setConnection(Connection conn) {
		this.connection = conn;
	}

	@Override
	public void configure(Context context) {
		factory = RabbitMQUtil.getFactory(context);
		queueName = context.getString(
				FlumeRabbitMQConstants.CONFIG_QUEUENAME, FlumeRabbitMQConstants.DEFAULT_QUEUE_NAME);
		exchangeName = context.getString(
				FlumeRabbitMQConstants.CONFIG_EXCHANGENAME, FlumeRabbitMQConstants.DEFAULT_EXCHANGE_NAME);
		batchSize = context.getInteger(
				FlumeRabbitMQConstants.CONFIG_BATCH_SIZE, 1);
		routingKey = context.getString(
				FlumeRabbitMQConstants.CONFIG_ROUTING_KEY, FlumeRabbitMQConstants.DEFAULT_ROUTING_KEY);
		confirmTimeout = context.getInteger(
				FlumeRabbitMQConstants.CONFIG_CONFIRM_TIMEOUT, FlumeRabbitMQConstants.DEFAULT_CONFIRM_TIMEOUT);
	}

	private void isRabbitMQConnected() {
		if(channel != null && channel.isOpen()) {
			return;
		}

		try {
			if(connection == null || !connection.isOpen()) {
				connection = factory.newConnection();
			}

			channel = connection.createChannel();
			channel.exchangeDeclare(exchangeName, "direct");
			channel.queueDeclare(queueName, true, false, false, null);
			channel.queueBind(queueName, exchangeName, routingKey);
			channel.confirmSelect();
			counterGroup.incrementAndGet(FlumeRabbitMQConstants.COUNTER_NEW_CHANNEL);
		} catch (Exception ex) {
			// if a channel could not be established on startup, stop the agent
			if (log.isErrorEnabled())
				log.error(this.getName()
						+ " - Exception while creating channel.", ex);
		}
	}

	@Override
	public synchronized void stop() {
		RabbitMQUtil.close(connection, channel);
		super.stop();
	}

	@Override
	public Status process() throws EventDeliveryException {
		isRabbitMQConnected();
		Transaction tx = getChannel().getTransaction();
		Status status = Status.READY;
		try {
			tx.begin();
			for(int i = 0; i < batchSize; i++){
				Event e = getChannel().take();
				
				if (e == null) {
					if (i == 0) status = Status.BACKOFF;
					break;
				}

				channel.basicPublish(exchangeName, routingKey,
						MessageProperties.PERSISTENT_TEXT_PLAIN, e.getBody());
				counterGroup.incrementAndGet(FlumeRabbitMQConstants.COUNTER_PUBLISH);
			}
			channel.waitForConfirms(confirmTimeout);
			tx.commit();
		} catch (Exception ex) {
			//TODO need to find a better way to differentiate different types of exceptions
			tx.rollback();

			if (log.isErrorEnabled())
				log.error(
						this.getName()
								+ " - error happens when sending message to rabbitMQ, will backoff a little bit", ex);
			status = Status.BACKOFF;
		} finally {
			tx.close();
		}

		return status;
	}
}
