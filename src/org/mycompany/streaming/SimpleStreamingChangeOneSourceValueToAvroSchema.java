package org.mycompany.streaming;

import java.util.Properties;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.mycompany.consumer.Consumer;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class SimpleStreamingChangeOneSourceValueToAvroSchema extends Consumer{

	
	private Properties props = new Properties();
	private StreamsConfig streamConfig;
	private KStreamBuilder builder = new KStreamBuilder();
	private KafkaStreams streams;
	private KStream<Long, String> rowsOfTopics;
	private String inTopic="";
	private String outTopic="";
	private final Serde<Long> outKeySerde = new Serdes.LongSerde();
    
	private  Injection<GenericRecord, byte[]> recordInjection;
    private int counter=0;
	
    private Schema schema;
    
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"name\", \"type\":\"string\" },"
            + "  { \"name\":\"counter\", \"type\":\"int\" }"
            + "]}";
 
    
	
	public static void main(String[] args) {
		SimpleStreamingChangeOneSourceValueToAvroSchema simple = new SimpleStreamingChangeOneSourceValueToAvroSchema("SimpleStreaming", "localhost:9092", "localhost:2181","kafka_accounts_more_complex","kafka_accounts_more_complex_out");
		simple.consume();
	}

	public SimpleStreamingChangeOneSourceValueToAvroSchema(String _applicationId, String _kafkaBrokers, String _zookeeper,String _inTopic, String _outTopic){
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, _applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokers);
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, _zookeeper);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");		
		inTopic = _inTopic;
		outTopic = _outTopic;
		Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(USER_SCHEMA);
        recordInjection = GenericAvroCodecs.toBinary(schema);
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
		KStream<Long, byte[]> nuovo  = rowsOfTopics.mapValues(new ValueMapper<String, byte[]>() {

			public byte[] apply(String value) {
					
				System.out.println("value "+value);
				GenericData.Record avroRecord = new GenericData.Record(schema);
				avroRecord.put("name", value);
				avroRecord.put("counter", ++counter);
				byte[] bytes = recordInjection.apply(avroRecord);
				
				return bytes;
			}
		});
		nuovo.through(outKeySerde,Serdes.ByteArray(),outTopic);
		nuovo.foreach(new ForeachAction<Long, byte[]>() {
			
			public void apply(Long key, byte[] value) {
				Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(USER_SCHEMA);
                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                GenericRecord record = recordInjection.invert(value).get();

                System.out.println("name= " + record.get("name")
                        	+ "\n counter= " + record.get("counter"));
				
			}
		});
	}
	

		
}
