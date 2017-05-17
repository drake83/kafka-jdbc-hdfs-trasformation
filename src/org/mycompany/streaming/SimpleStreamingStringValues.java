package org.mycompany.streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.mycompany.consumer.Consumer;

public class SimpleStreamingStringValues extends Consumer{

	
	private Properties props = new Properties();
	private StreamsConfig streamConfig;
	private KStreamBuilder builder = new KStreamBuilder();
	private KafkaStreams streams;
	private KStream<String, String> rowsOfTopics;
	private String inTopic="";
	private String outTopic=""; 
	
	public static void main(String[] args) {
		SimpleStreamingStringValues simple = new SimpleStreamingStringValues("SimpleStreaming", "localhost:9092", "localhost:2181","simple_streaming_in","simple_streaming_out");
		simple.consume();
	}

	public SimpleStreamingStringValues(String _applicationId, String _kafkaBrokers, String _zookeeper,String _inTopic, String _outTopic){
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, _applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokers);
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, _zookeeper);
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");		
		inTopic = _inTopic;
		outTopic = _outTopic;
	}
	

	@Override
	protected void setupConsumer() {
		streamConfig = new StreamsConfig(props);
		rowsOfTopics = builder.stream(inTopic);
	}

	@Override
	protected void closeConsumption() {
		
		 Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
		      public void run() {
		        streams.close();
		      }
		    }));

	}

	@Override
	protected void doComsumption() {
		transformSourceTopic();
		streams = new KafkaStreams(builder, streamConfig);
		streams.start();		
	}
	
	private void transformSourceTopic (){
		rowsOfTopics.mapValues(new ValueMapper<String, String>() {

			public String apply(String originalValue) {
				String changedValue = originalValue+"aggiuntina";
				System.out.println(changedValue);
				return changedValue;
			}
		}).through(outTopic);
	}
		
}
