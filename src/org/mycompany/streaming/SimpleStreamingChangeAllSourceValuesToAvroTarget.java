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
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.mycompany.consumer.Consumer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import utils.GenericAvroSerde;

public class SimpleStreamingChangeAllSourceValuesToAvroTarget extends Consumer{

	
	private Properties props = new Properties();
	private StreamsConfig streamConfig;
	private KStreamBuilder builder = new KStreamBuilder();
	private KafkaStreams streams;	
	private KTable<Long, GenericRecord> rowsOfTopics;
	private String inTopic="";
	private String outTopic="";
	private Serde<Long> keySerde = new Serdes.LongSerde();
    private Serde<byte[]> valueSerde = new Serdes.ByteArraySerde();  
	private Injection<GenericRecord, byte[]> targetRecordInjection;
    private Schema targetSchema;
    private static final String TARGET_USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"id\", \"type\":\"int\" },"
            + "  { \"name\":\"name\", \"type\":\"string\" },"
            + "  { \"name\":\"counter\", \"type\":\"int\" }"
            + "]}";
    private int contatore = 0;
    
	
	public static void main(String[] args) {
		SimpleStreamingChangeAllSourceValuesToAvroTarget simple = new SimpleStreamingChangeAllSourceValuesToAvroTarget(args[0], args[1], args[2],args[3],args[4],args[5]);
		simple.consume();
	}

	public SimpleStreamingChangeAllSourceValuesToAvroTarget(String _applicationId, String _kafkaBrokers,String _schemaRegistryUrl, String _zookeeper,String _inTopic, String _outTopic){
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, _applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokers);
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, _zookeeper);
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);		
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");		
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, _schemaRegistryUrl);
		inTopic = _inTopic;
		outTopic = _outTopic;
		Schema.Parser parser = new Schema.Parser();
        targetSchema = parser.parse(TARGET_USER_SCHEMA);
        targetRecordInjection = GenericAvroCodecs.toBinary(targetSchema);
	}
	

	@Override
	protected void setupConsumer() {
		streamConfig = new StreamsConfig(props);
		rowsOfTopics = builder.table(inTopic);
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
				
		KTable<Long, byte[]> nuovo  = rowsOfTopics.mapValues(new ValueMapper<GenericRecord, byte[]>() {

			public byte[] apply(GenericRecord value) {

				String name = (String) value.get("name").toString();
				Long val2 = (Long) value.get("val2");
								
				//creo il target con i dati del source
				GenericData.Record recordTarget = new GenericData.Record(targetSchema);
				recordTarget.put("id",++contatore);
				recordTarget.put("name", name);
				recordTarget.put("counter", val2);
				byte[] bytes = targetRecordInjection.apply(recordTarget);
				
				return bytes;
			}
		});
		nuovo.through(keySerde,valueSerde,outTopic);
		
		nuovo.foreach(new ForeachAction<Long, byte[]>() {
			
			public void apply(Long key, byte[] value) {
                GenericRecord record = targetRecordInjection.invert(value).get();

                System.out.println("name= " + record.get("name")
                        	+ "\n counter= " + record.get("counter"));
				
			}
		});
	}
	

		
}
