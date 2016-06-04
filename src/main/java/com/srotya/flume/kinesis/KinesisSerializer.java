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

/**
 * @author ambudsharma
 */
public interface KinesisSerializer {
	
	/**
	 * Configure  
	 * 
	 * @param conf
	 */
	public void configure(Map<String, String> conf); 
	
	/**
	 * Serialize event to {@link ByteBuffer}
	 * 
	 * @param event
	 * @return buffer
	 */
	public ByteBuffer serialize(Event event);
	
	/**
	 * Deserialize event from {@link ByteBuffer}
	 * 
	 * @param buffer
	 * @return event
	 */
	public Event deserialize(ByteBuffer buffer);

}
