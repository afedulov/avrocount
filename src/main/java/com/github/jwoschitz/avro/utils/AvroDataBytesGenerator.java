package com.github.jwoschitz.avro.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.ByteArrayOutputStream;
import java.util.function.BiFunction;

public class AvroDataBytesGenerator {
    private final Schema schema;
    private final CodecFactory codecFactory;
    private final BiFunction<Schema, Long, GenericRecord> recordCreatorFn;

    public AvroDataBytesGenerator( Schema schema, BiFunction<Schema, Long, GenericRecord> recordCreatorFn, CodecFactory codecFactory) {
        this.schema = schema;
        this.codecFactory = codecFactory;
        this.recordCreatorFn = recordCreatorFn;
    }

    public byte[] createAvroBytes(long recordCount) throws Exception {
        ByteArrayOutputStream target = new ByteArrayOutputStream();

        try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            if (codecFactory != null) {
                writer.setCodec(codecFactory);
            }
            writer.create(schema, target);

            for (long i = 0; i < recordCount; i++) {
                writer.append(recordCreatorFn.apply(schema, i));
            }
        }
        return target.toByteArray();
    }

    public static AvroDataBytesGenerator intRecordGenerator(CodecFactory codec) throws Exception {
        return new AvroDataBytesGenerator(
                new Schema.Parser().parse(AvroDataBytesGenerator.class.getClassLoader().getResourceAsStream("intRecord.avsc")),
                (schema, value) -> new GenericRecordBuilder(schema)
                        .set("value", value)
                        .build(),
                codec
        );
    }
}
