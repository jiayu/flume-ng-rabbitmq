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

package org.apache.flume;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class RabbitMQUtil {
    private static final Logger log = LoggerFactory.getLogger(RabbitMQUtil.class);
    static final String PREFIX="RabbitMQ";
    
    private static void setTimestamp(Map<String,String> headers, BasicProperties properties){
        Date date = properties.getTimestamp()==null?new Date():properties.getTimestamp();        
        Long value=date.getTime();
        headers.put("timestamp", value.toString());
    }
    
    public static Map<String,String> getHeaders(BasicProperties properties){
        Preconditions.checkArgument(properties!=null, "properties cannot be null.");
        Map<String,String> headers = new CaseInsensitiveMap();
        setTimestamp(headers, properties);
        
        Map<String, Object> rabbitmqHeaders = properties.getHeaders();
        
        if(null!=rabbitmqHeaders){
            for(Map.Entry<String, Object> kvp:rabbitmqHeaders.entrySet()){
                if(!headers.containsKey(kvp.getKey())&&null!=kvp.getValue()){
                    if(log.isInfoEnabled())log.info("header=" + kvp.getKey() + " value=" + kvp.getValue());
                    headers.put(kvp.getKey(), kvp.getValue().toString());
                }
            }
        }
        
        return headers;
    }
    
    public static String getQueueName(Context context) {
        return context.getString(FlumeRabbitMQConstants.CONFIG_QUEUENAME, "");
    }
    
    public static String getExchangeName(Context context){
        return context.getString(FlumeRabbitMQConstants.CONFIG_EXCHANGENAME, "");
    }

    public static String[] getTopics( Context context ) {
    	String list = context.getString( FlumeRabbitMQConstants.CONFIG_TOPICS, "" );

    	if ( !list.equals("") ) {
    		return list.split(",");
    	}

    	return null;
    }
    
    public static ConnectionFactory getFactory(Context context){
    	//Hostname has to be set
		Preconditions.checkArgument(context != null, "context cannot be null.");
		Preconditions.checkArgument(context.getString("hostname") != null, "No hostname specified.");

		String hostname = context.getString("hostname");
		int port = context.
				getInteger(FlumeRabbitMQConstants.CONFIG_PORT, ConnectionFactory.DEFAULT_AMQP_PORT);
		String username = context.
				getString(FlumeRabbitMQConstants.CONFIG_USERNAME, ConnectionFactory.DEFAULT_USER);
		String password = context.
				getString(FlumeRabbitMQConstants.CONFIG_PASSWORD, ConnectionFactory.DEFAULT_PASS);
		String virtualHost = context
				.getString(FlumeRabbitMQConstants.CONFIG_VIRTUALHOST, ConnectionFactory.DEFAULT_VHOST);
		int connectionTimeout = context.
				getInteger(FlumeRabbitMQConstants.CONFIG_CONNECTIONTIMEOUT, ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT);

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(hostname);
		factory.setPort(port);
		factory.setUsername(username);
		factory.setPassword(password);
		factory.setVirtualHost(virtualHost);
		factory.setConnectionTimeout(connectionTimeout);
		factory.setAutomaticRecoveryEnabled(true);
		factory.setNetworkRecoveryInterval(60 * 1000); //auto recovery time interval set to 1 min
        
        return factory;
    }
    
    public static void close(Connection connection, com.rabbitmq.client.Channel channel){
        if(null!=channel) {
            try {
                channel.close();
            } catch(Exception ex){
                if(log.isErrorEnabled())log.error("Exception thrown while closing channel", ex);
            }finally{
            	channel = null;
            }
        }
        
        if(null!=connection) {
            try {
                connection.close();
            } catch(Exception ex){
                if(log.isErrorEnabled())log.error("Exception thrown while closing connection", ex);
            }finally{
            	connection = null;
            }
        }
    }
}
