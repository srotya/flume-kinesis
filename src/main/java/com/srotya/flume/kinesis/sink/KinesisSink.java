/**
 * Copyright 2016 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.flume.kinesis.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.srotya.flume.kinesis.Constants;
import com.srotya.flume.kinesis.GsonSerializer;
import com.srotya.flume.kinesis.KinesisSerializer;

/**
 * Kinesis Sink is an implementation of an Apache Flume {@link AbstractSink}
 * that can be used to push data into a configured Kinesis stream. There's also
 * options for configuring custom partition and hash keys as well as custom hash
 * partition keys. Lastly serializer for data is also configurable.
 * 
 * @author ambudsharma
 */
public class KinesisSink extends AbstractSink implements Configurable {

	
	private String partitionKey;
	private String hashKey;
	private ClientConfiguration clientConfig;
	private AWSCredentials awsCredentials;
	private AmazonKinesisClient client;
	private String streamName;
	private KinesisSerializer serializer;
	private int putSize;
	private String endpoint;

	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel ch = getChannel();
		Transaction tx = ch.getTransaction();
		tx.begin();
		try {
			PutRecordsRequest request = new PutRecordsRequest();
			List<PutRecordsRequestEntry> records = new ArrayList<>();
			for (int i = 0; i < putSize; i++) {
				Event event = ch.take();
				PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
				if (partitionKey != null) {
					entry.setPartitionKey(event.getHeaders().get(partitionKey));
				}else {
					entry.setPartitionKey(String.format("partitionKey-%d", i));
				}
				if (hashKey != null) {
					entry.setExplicitHashKey(event.getHeaders().get(hashKey));
				}
				entry.setData(serializer.serialize(event));
				records.add(entry);
			}
			request.setRecords(records);
			request.setStreamName(streamName);
			client.putRecords(request);
			tx.commit();
		} catch (Throwable e) {
			e.printStackTrace();
			tx.rollback();
			status = Status.BACKOFF;
		} finally {
			try {
				tx.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return status;
	}

	@Override
	public synchronized void start() {
		client = new AmazonKinesisClient(awsCredentials, clientConfig);
		client.setEndpoint(endpoint);
		super.start();
	}

	@Override
	public void configure(Context ctx) {
		ImmutableMap<String, String> props = ctx.getSubProperties(Constants.SETTINGS);
		if (!props.containsKey(Constants.ACCESS_KEY) || !props.containsKey(Constants.ACCESS_SECRET)) {
			Throwables.propagate(
					new InvalidArgumentException("Must provide AWS credentials i.e. accessKey and accessSecret"));
		}
		awsCredentials = new BasicAWSCredentials(props.get(Constants.ACCESS_KEY), props.get(Constants.ACCESS_SECRET));
		clientConfig = new ClientConfiguration();
		if (props.containsKey(Constants.PROXY_HOST)) {
			clientConfig.setProxyHost(props.get(Constants.PROXY_HOST));
			clientConfig.setProxyPort(Integer.parseInt(props.getOrDefault(Constants.PROXY_PORT, "80")));
			clientConfig.setProtocol(Protocol.valueOf(props.getOrDefault(Constants.PROTOCOL, "HTTPS")));
		}
		if (!props.containsKey(Constants.STREAM_NAME)) {
			Throwables.propagate(new InvalidArgumentException("Must provide Kinesis stream name"));
		}
		streamName = props.get(Constants.STREAM_NAME);
		putSize = Integer.parseInt(props.getOrDefault(Constants.PUT_SIZE, "100"));
		if(putSize>500) {
			Throwables.propagate(
					new InvalidArgumentException("AWS Kinesis doesn't allow more than 500 put requests"));
		}
		endpoint = props.getOrDefault(Constants.ENDPOINT, Constants.DEFAULT_ENDPOINT);
		String serializerClass = props.getOrDefault(Constants.SERIALIZER, GsonSerializer.class.getName());
		try {
			serializer = (KinesisSerializer) Class.forName(serializerClass).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			Throwables.propagate(e);
		}
		serializer.configure(props);
	}

	@Override
	public synchronized void stop() {
		client.shutdown();
	}
	
	/**
	 * @return the partitionKey
	 */
	protected String getPartitionKey() {
		return partitionKey;
	}

	/**
	 * @param partitionKey the partitionKey to set
	 */
	protected void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	/**
	 * @return the hashKey
	 */
	protected String getHashKey() {
		return hashKey;
	}

	/**
	 * @param hashKey the hashKey to set
	 */
	protected void setHashKey(String hashKey) {
		this.hashKey = hashKey;
	}

	/**
	 * @return the clientConfig
	 */
	protected ClientConfiguration getClientConfig() {
		return clientConfig;
	}

	/**
	 * @param clientConfig the clientConfig to set
	 */
	protected void setClientConfig(ClientConfiguration clientConfig) {
		this.clientConfig = clientConfig;
	}

	/**
	 * @return the awsCredentials
	 */
	protected AWSCredentials getAwsCredentials() {
		return awsCredentials;
	}

	/**
	 * @param awsCredentials the awsCredentials to set
	 */
	protected void setAwsCredentials(AWSCredentials awsCredentials) {
		this.awsCredentials = awsCredentials;
	}

	/**
	 * @return the client
	 */
	protected AmazonKinesisClient getClient() {
		return client;
	}

	/**
	 * @param client the client to set
	 */
	protected void setClient(AmazonKinesisClient client) {
		this.client = client;
	}

	/**
	 * @return the streamName
	 */
	protected String getStreamName() {
		return streamName;
	}

	/**
	 * @param streamName the streamName to set
	 */
	protected void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	/**
	 * @return the serializer
	 */
	protected KinesisSerializer getSerializer() {
		return serializer;
	}

	/**
	 * @param serializer the serializer to set
	 */
	protected void setSerializer(KinesisSerializer serializer) {
		this.serializer = serializer;
	}

	/**
	 * @return the putSize
	 */
	protected int getPutSize() {
		return putSize;
	}

	/**
	 * @param putSize the putSize to set
	 */
	protected void setPutSize(int putSize) {
		this.putSize = putSize;
	}

}