package utils;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class SpecificAvroSerde<T extends  org.apache.avro.specific.SpecificRecord> implements Serde<T> {

  private final Serde<T> inner;

  /**
   * Constructor used by Kafka Streams.
   */
  public SpecificAvroSerde() {
    inner = (Serde<T>) Serdes.serdeFrom(new SpecificAvroSerializer<SpecificRecord>(), new SpecificAvroDeserializer<SpecificRecord>());
  }

  public SpecificAvroSerde(SchemaRegistryClient client) {
    this();
  }

  public SpecificAvroSerde(SchemaRegistryClient client, Map<String, ?> props) {
    inner = (Serde<T>) Serdes.serdeFrom(new SpecificAvroSerializer<SpecificRecord>(client, props), new SpecificAvroDeserializer<SpecificRecord>(client, props));
  }

  public Serializer<T> serializer() {
    return inner.serializer();
  }

  public Deserializer<T> deserializer() {
    return inner.deserializer();
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}