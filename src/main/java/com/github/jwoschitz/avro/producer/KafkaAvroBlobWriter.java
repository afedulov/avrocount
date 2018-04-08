package com.github.jwoschitz.avro.producer;

import com.github.jwoschitz.avro.utils.AvroDataBytesGenerator;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import static com.github.jwoschitz.avro.utils.AvroDataBytesGenerator.intRecordGenerator;

import java.util.Properties;
import java.util.function.BiFunction;

public class KafkaAvroBlobWriter {

    public static void main(String[] args) throws Exception {
        System.out.println("Producing Avro blobs!");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        AvroDataBytesGenerator generator = intRecordGenerator(CodecFactory.nullCodec());

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>("avroblob", generator.createAvroBytes(1000));
        producer.send(record);

        producer.close();
    }
}
