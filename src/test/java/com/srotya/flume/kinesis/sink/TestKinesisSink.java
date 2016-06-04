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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.google.common.collect.ImmutableMap;
import com.srotya.flume.kinesis.GsonSerializer;

/**
 * Adding mock tests for {@link KinesisSink}
 * 
 * @author ambudsharma
 */
@RunWith(MockitoJUnitRunner.class)
public class TestKinesisSink {
	
	@Mock
	AmazonKinesisClient client;
	@Mock
	Context ctx;
	@Mock
	Channel channel;
	@Mock
	Transaction transaction;
	ImmutableMap<String, String> map;
	
	@Before
	public void before() {
		map = ImmutableMap.<String, String>builder()
				.put("accessKey", "234234234")
				.put("accessSecret", "345t34252345")
				.put("streamName", "test")
				.build(); 
	}
	
	@Test
	public void testConfigure() {
		when(ctx.getSubProperties("settings.")).thenReturn(map);
		KinesisSink sink = new KinesisSink();
		sink.configure(ctx);
		assertEquals(map.get("accessKey"), sink.getAwsCredentials().getAWSAccessKeyId());
		assertEquals(map.get("accessSecret"), sink.getAwsCredentials().getAWSSecretKey());
		assertEquals(GsonSerializer.class, sink.getSerializer().getClass());
	}

	@Test
	public void testProcess() throws EventDeliveryException {
		byte[] body = "these are test bytes".getBytes();
		Map<String, String> headers = new HashMap<>();
		TestEvent event = new TestEvent(headers, body);
		when(channel.take()).thenReturn(event);
		when(channel.getTransaction()).thenReturn(transaction);
		when(ctx.getSubProperties("settings.")).thenReturn(map);
		
		KinesisSink sink = new KinesisSink();
		sink.configure(ctx);
		sink.setClient(client);
		sink.setChannel(channel);
		sink.process();
		verify(client, times(1)).putRecords(any(PutRecordsRequest.class));
		verify(channel, times(100)).take();
	}
	
	public static class TestEvent implements Event {

		private Map<String, String> headers;
		private byte[] body;
		
		public TestEvent(Map<String, String> headers, byte[] body) {
			this.headers = headers;
			this.body = body;
		}
		
		@Override
		public Map<String, String> getHeaders() {
			return headers;
		}

		@Override
		public void setHeaders(Map<String, String> headers) {
			this.headers = headers;
		}

		@Override
		public byte[] getBody() {
			return body;
		}

		@Override
		public void setBody(byte[] body) {
		}
		
	}
	
}
