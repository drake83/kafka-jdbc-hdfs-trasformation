package org.mycompany.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class StringConsumer extends Consumer{

	private Properties props;
	private String topic;
	private KafkaConsumer<String, String> consumer;
	
	public StringConsumer(String _host, int _port,String _groupId, String _topic){
		props = new Properties();
		props.put("bootstrap.servers", _host+":"+_port);
		props.put("group.id", _groupId);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		topic = _topic;
	}
	
	
	@Override
	protected void setupConsumer(){		
		consumer= new KafkaConsumer<String, String>(props);
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
        	ConsumerRecords<String, String> records = consumer.poll(100);
        	for (ConsumerRecord<String, String> record:records){
        		System.out.println(record.value()+1);        		
        	}        	
        }		
	}
	
	public static void main(String[] args){
		new StringConsumer("localhost", 9092, "group1", "test-sqlite-jdbc-accounts").consume();
	}
	
}
