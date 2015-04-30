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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;

public class RabbitMQSinkTestApp {

	private RabbitMQSink sink;
	private Channel ch;
	
	public static void main(String[] args) throws EventDeliveryException, InterruptedException {
		RabbitMQSinkTestApp app = new RabbitMQSinkTestApp();
		app.ch.start();
		app.sink.start();
		
		int count = 10;
		while(true){
			if(count == 0) break;
			
			app.feedsFlChannel(50);
			app.sink.process();
			count--;
			Thread.sleep(60 * 1000);
		}
		
		app.ch.stop();
		app.sink.stop();
	}
	
	public RabbitMQSinkTestApp(){
		sink = new RabbitMQSink();
		ch = new MemoryChannel();
		Context ct = getContext();
		Configurables.configure(ch, ct);
		Configurables.configure(sink, ct);
		sink.setChannel(ch);
	}
	
	private void feedsFlChannel(int num) {
		Transaction tx = ch.getTransaction();
		tx.begin();
		for (int i = 0; i < num; i++) {
			Event e = new SimpleEvent();
			e.setBody("Hello".getBytes());
			ch.put(e);
		}
		tx.commit();
		tx.close();
	}
	
	private Context getContext() {
		Context context = new Context();
		context.put("hostname", "localhost");
		context.put("queuename", "test_queue");
		context.put("exchangename", "test_exchange");
		context.put("batchsize", "50");
		context.put("capacity", "1000");
		return context;
	}
}
