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
package com.srotya.flume.kinesis;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.flume.Event;

import com.google.gson.Gson;

/**
 * Json {@link KinesisSerializer} implementation using Gson
 * 
 * @author ambudsharma
 */
public class GsonSerializer implements KinesisSerializer {

	private Gson gson;

	public GsonSerializer() {
	}
	
	@Override
	public ByteBuffer serialize(Event event) {
		String json = gson.toJson(event);
		return ByteBuffer.wrap(json.getBytes());
	}

	@Override
	public Event deserialize(ByteBuffer buffer) {
		Event event = gson.fromJson(new String(buffer.array()), Event.class);
		return event;
	}

	@Override
	public void configure(Map<String, String> conf) {
		gson = new Gson();
	}

}
