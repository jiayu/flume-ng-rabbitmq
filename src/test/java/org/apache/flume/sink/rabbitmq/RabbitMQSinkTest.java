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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeRabbitMQConstants;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

public class RabbitMQSinkTest {
	private static int NUM_OF_EVENTS = 10;
	private static String EXCHANGE_NAME = "exchangeName";
	private static String QUEUE_NAME = "queueName";
	private static String ROUTING_KEY = "routingKey";
	private static int BATCH_SIZE = 5;
	private RabbitMQSink sink;
	private org.apache.flume.Channel flChannel;
	private com.rabbitmq.client.Channel rbChannel;
	private Connection connection;
	private Context context;
	
	@Rule
	public ExpectedException thrown= ExpectedException.none();

	@Before
	public void setUp() throws Exception {
		// RabbitMQ mock
		connection = mock(Connection.class);
		rbChannel = mock(com.rabbitmq.client.Channel.class);
		when(connection.createChannel()).thenReturn(rbChannel);
		when(connection.isOpen()).thenReturn(true);
		
		// Flume setup
		sink = new RabbitMQSink();
		flChannel = new MemoryChannel();
		initContext();
		Configurables.configure(flChannel, context);
		Configurables.configure(sink, context);
		sink.setChannel(flChannel);
		sink.setConnection(connection);
	}

	private void initContext() {
		context = new Context();
		context.put("hostname", "localhost");
		context.put("queuename", QUEUE_NAME);
		context.put("username", "test_user");
		context.put("password", "password");
		context.put("port", "4123");
		context.put("virtualhost", "virtual_host");
		context.put("exchangename", EXCHANGE_NAME);
		context.put("batchsize", String.valueOf(BATCH_SIZE));
		context.put("routingkey", ROUTING_KEY);
	}

	private void feedsFlChannel(int num) {
		Transaction tx = flChannel.getTransaction();
		tx.begin();
		for (int i = 0; i < num; i++) {
			Event e = new SimpleEvent();
			e.setBody("Hello".getBytes());
			flChannel.put(e);
		}
		tx.commit();
		tx.close();
	}

	/**
	 * An ugly way to count the number of events in the channel
	 * 
	 * @return int
	 */
	private int getNumOfEventsInChannel() {
		Transaction tx = flChannel.getTransaction();
		tx.begin();
		int count = 0;
		while (flChannel.take() != null)
			count++;
		tx.rollback();
		tx.close();
		return count;
	}

	private void assertEverythingIsClosed() throws IOException {
		verify(connection).close();
		verify(rbChannel).close();
	}

	@Test
	public void lessEventsInChannelThanBatchSize() throws EventDeliveryException, IOException {
		feedsFlChannel(BATCH_SIZE - 3);
		Status status = sink.process();

		verify(rbChannel, times(BATCH_SIZE - 3)).basicPublish(EXCHANGE_NAME, ROUTING_KEY,
				MessageProperties.PERSISTENT_TEXT_PLAIN, "Hello".getBytes());
		assertEquals(Status.READY, status);
		assertEquals(0, getNumOfEventsInChannel());

		sink.stop();
		assertEverythingIsClosed();
	}
	
	@Test
	public void consumeByBatchSize() throws EventDeliveryException, IOException {
		feedsFlChannel(NUM_OF_EVENTS);
		Status status = sink.process();

		verify(rbChannel, times(BATCH_SIZE)).basicPublish(EXCHANGE_NAME, ROUTING_KEY,
				MessageProperties.PERSISTENT_TEXT_PLAIN, "Hello".getBytes());
		assertEquals(Status.READY, status);
		assertEquals(NUM_OF_EVENTS - BATCH_SIZE, getNumOfEventsInChannel());

		sink.stop();
		assertEverythingIsClosed();
	}

	@Test
	public void consumeAllEvents() throws EventDeliveryException, IOException {
		feedsFlChannel(NUM_OF_EVENTS);
		sink.process();
		Status status = sink.process();

		verify(rbChannel, times(2 * BATCH_SIZE)).basicPublish(EXCHANGE_NAME, ROUTING_KEY,
				MessageProperties.PERSISTENT_TEXT_PLAIN, "Hello".getBytes());
		assertEquals(Status.READY, status);
		assertEquals(NUM_OF_EVENTS - 2 * BATCH_SIZE, getNumOfEventsInChannel());

		sink.stop();
		assertEverythingIsClosed();
	}

	
	@Test
	public void noEventInQueue() throws EventDeliveryException, IOException {
		Status status = sink.process();
		sink.stop();
		assertEquals(0, getNumOfEventsInChannel());
		assertEquals(Status.BACKOFF, status);
		verify(rbChannel, times(0)).basicPublish(EXCHANGE_NAME, ROUTING_KEY,
				MessageProperties.PERSISTENT_TEXT_PLAIN, "Hello".getBytes());
	}
	
	@Test
	public void exceptionWhenPublish() throws IOException,
			EventDeliveryException {
		feedsFlChannel(NUM_OF_EVENTS);
		
		doThrow(new IOException()).when(rbChannel).basicPublish(EXCHANGE_NAME,
				ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN,
				"Hello".getBytes());
		
		Status status =	sink.process();
		assertEquals(Status.BACKOFF, status);
		assertEquals(NUM_OF_EVENTS, getNumOfEventsInChannel());
	}
	
	@Test
	public void exceptionWhenWaiting() throws InterruptedException,
			EventDeliveryException, TimeoutException {
		feedsFlChannel(NUM_OF_EVENTS);
		
		doThrow(new TimeoutException()).when(rbChannel).waitForConfirms(FlumeRabbitMQConstants.DEFAULT_CONFIRM_TIMEOUT);

		Status status = sink.process();
		assertEquals(Status.BACKOFF, status);
		assertEquals(getNumOfEventsInChannel(), NUM_OF_EVENTS);
	}
}
