# flume-kinesis
AWS Kinesis Source and sink for Apache Flume for use to dump data to and from AWS Kinesis.

```java
This project is compatible with Apache Flume 1.6.0 or higher
```

### Implementations
This project will support 2 types of Kinesis Source / Sink implementations:
1. API
2. KCL

API based implementation is done, KCL will be added soon!

### Usage
Download Apache Flume from: http://www.apache.org/dyn/closer.lua/flume/1.6.0/apache-flume-1.6.0-bin.tar.gz

Examples below assume that your agent is named "agent" and channel is "memoryChannel"

#### Source
```java
agent.sources.kinesisSource.type = com.srotya.flume.kinesis.source.KinesisSource
agent.sources.kinesisSource.channels = memoryChannel
agent.sources.kinesisSource.settings.accessSecret = <your access secret>
agent.sources.kinesisSource.settings.accessKey = <your access key>
agent.sources.kinesisSource.settings.streamName = <your stream name>
```

#### Sink
```java
agent.sinks.kinesisSink.type = com.srotya.flume.kinesis.sink.KinesisSink
agent.sinks.kinesisSink.channel = memoryChannel
agent.sinks.kinesisSink.settings.accessSecret = <your access secret>
agent.sinks.kinesisSink.settings.accessKey = <your access key>
agent.sinks.kinesisSink.settings.streamName = <your stream name>
```

### Wiki


### License
This project is licensed under Apache 2.0

Copyright 2016 Ambud Sharma

Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.