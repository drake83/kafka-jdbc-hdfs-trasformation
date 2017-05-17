package org.mycompany.streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.mycompany.consumer.Consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class SimpleStreamingChangeSourceValue extends Consumer{

	
	private Properties props = new Properties();
	private StreamsConfig streamConfig;
	private KStreamBuilder builder = new KStreamBuilder();
	private KafkaStreams streams;
	private KStream<Long, String> rowsOfTopics;
	private String inTopic="";
	private String outTopic="";
	final Serde<Long> keySerde = new Serdes.LongSerde();
    final Serde<String> valueSerde = new Serdes.StringSerde();    
	
	public static void main(String[] args) {
		SimpleStreamingChangeSourceValue simple = new SimpleStreamingChangeSourceValue("SimpleStreaming", "localhost:9092", "localhost:2181","kafka_accounts","kafka_accounts_out");
		simple.consume();
	}

	public SimpleStreamingChangeSourceValue(String _applicationId, String _kafkaBrokers, String _zookeeper,String _inTopic, String _outTopic){
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, _applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokers);
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, _zookeeper);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
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
		KStream<Long, String> nuovo  = rowsOfTopics.mapValues(new ValueMapper<String, String>() {

			public String apply(String value) {
								
				return value+"aggiuntina";
			}
		});
		nuovo.through(keySerde,valueSerde,outTopic);
		nuovo.print();
	}
	

		
}
