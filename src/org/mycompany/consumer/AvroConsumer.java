package org.mycompany.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.avro.generic.GenericRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class AvroConsumer extends Consumer{

	private Properties props;
	private String topic;
	private KafkaConsumer<String, byte[]> consumer;
	private static final String USER_SCHEMA = "{"
	            + "\"type\":\"record\","
	            + "\"name\":\"myrecord\","
	            + "\"fields\":["
	            + "  { \"name\":\"str1\", \"type\":\"string\" },"
	            + "  { \"name\":\"str2\", \"type\":\"string\" },"
	            + "  { \"name\":\"int1\", \"type\":\"int\" }"
	            + "]}";
	
	public AvroConsumer(String _host, int _port,String _groupId, String _topic){
		props = new Properties();
		props.put("bootstrap.servers", _host+":"+_port);
		props.put("group.id", _groupId);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		topic = _topic;
	}
	
	
	@Override
	protected void setupConsumer(){
		consumer= new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Arrays.asList(topic));	        
	}

	@Override
	protected void closeConsumption(){
		consumer.close();
	}

	@Override
	protected void doComsumption(){
        boolean running = true;        
        while (running){
        	ConsumerRecords<String, byte[]> records = consumer.poll(100);
        	for (ConsumerRecord<String, byte[]> record:records){ 
        		System.out.println("1");
        		Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(USER_SCHEMA);
                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                GenericRecord genericRecord = recordInjection.invert(record.value()).get();
        		
                System.out.println("str1= " + genericRecord.get("str1")
                + ", str2= " + genericRecord.get("str2")
                + ", int1=" + genericRecord.get("int1"));
        	}        	
        }		
	}
	
	public static void main(String[] args){
		new AvroConsumer("localhost", 9092, "group1", "mytopic_avro").consume();
	}
	
}
