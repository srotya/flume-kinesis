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
package com.srotya.flume.kinesis.source;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.srotya.flume.kinesis.Constants;
import com.srotya.flume.kinesis.GsonSerializer;
import com.srotya.flume.kinesis.KinesisSerializer;

/**
 * Simple KinesisSource for Apche Flume, that allows you to select shard and
 * auto compensates for the iterators.
 * 
 * @author ambudsharma
 */
public class KinesisSource extends AbstractPollableSource {

	private static final Logger logger = Logger.getLogger(KinesisSource.class);
	private static final String DEFAULT_ITERATOR_TYPE = "TRIM_HORIZON";
	private static final String ITERATOR_TYPE = "iteratorType";
	private static final String SHARD_INDEX = "shardIndex";
	private ClientConfiguration clientConfig;
	private AWSCredentials awsCredentials;
	private AmazonKinesisClient client;
	private String streamName;
	private KinesisSerializer serializer;
	private int putSize;
	private String endpoint;
	private String shardIterator;
	private List<Shard> shards;
	private int shardId;
	private String shardIteratorType;

	@Override
	protected Status doProcess() throws EventDeliveryException {
		Status status = Status.READY;
		GetRecordsRequest recordRequest = new GetRecordsRequest();
		recordRequest.setShardIterator(shardIterator);
		recordRequest.setLimit(putSize);
		GetRecordsResult records = client.getRecords(recordRequest);
		for (Record record : records.getRecords()) {
			try {
				getChannelProcessor().processEvent(serializer.deserialize(record.getData()));
			} catch (Exception e) {
				logger.error("Failed to deserialize event:" + new String(record.getData().array()), e);
			}
		}
		shardIterator = records.getNextShardIterator();
		if (shardIterator == null) {
			getShardIterator();
		}
		return status;
	}

	@Override
	protected void doConfigure(Context ctx) throws FlumeException {
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
		if (putSize > 500) {
			Throwables.propagate(new InvalidArgumentException("AWS Kinesis doesn't allow more than 500 put requests"));
		}
		endpoint = props.getOrDefault(Constants.ENDPOINT, Constants.DEFAULT_ENDPOINT);
		String serializerClass = props.getOrDefault(Constants.SERIALIZER, GsonSerializer.class.getName());
		try {
			serializer = (KinesisSerializer) Class.forName(serializerClass).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			Throwables.propagate(e);
		}
		serializer.configure(props);
		shardId = Integer.parseInt(props.getOrDefault(SHARD_INDEX, "0"));
		shardIteratorType = props.getOrDefault(ITERATOR_TYPE, DEFAULT_ITERATOR_TYPE);
	}

	@Override
	protected void doStart() throws FlumeException {
		client = new AmazonKinesisClient(awsCredentials, clientConfig);
		client.setEndpoint(endpoint);
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(streamName);
		shards = new ArrayList<>();
		String exclusiveStartShardId = null;
		do {
			describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
			DescribeStreamResult describeStreamResult = client.describeStream(describeStreamRequest);
			shards.addAll(describeStreamResult.getStreamDescription().getShards());
			if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
				exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
			} else {
				exclusiveStartShardId = null;
			}
		} while (exclusiveStartShardId != null);
		getShardIterator();
	}

	protected void getShardIterator() {
		System.out.println("Listing shards:");
		shards.forEach(shard -> System.out.println("Shard id:" + shard.getShardId()));
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		getShardIteratorRequest.setStreamName(streamName);
		getShardIteratorRequest.setShardId(shards.get(shardId).getShardId());
		getShardIteratorRequest.setShardIteratorType(shardIteratorType);

		GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
		shardIterator = getShardIteratorResult.getShardIterator();
	}

	@Override
	protected void doStop() throws FlumeException {
		client.shutdown();
	}

}