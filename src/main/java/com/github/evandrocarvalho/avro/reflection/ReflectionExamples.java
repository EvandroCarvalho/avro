package com.github.evandrocarvalho.avro.reflection;

import com.github.evandrocarvalho.avro.generic.GenericRecordExamples;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * @author Evandro Carvalho
 */
public class ReflectionExamples {
    private final static Logger logger = LoggerFactory.getLogger(ReflectionExamples.class);

    public static void main(String[] args) throws IOException {

        // here we use reflection to determine the schema
        Schema schema = ReflectData.get().getSchema(ReflectedCustomer.class);
        System.out.println("schema = " + schema.toString(true));
        Path path = Paths.get("src","main","resources", "avro","customer-reflection-example.avsc");
        Files.deleteIfExists(path);
        try (FileWriter fileWriter = new FileWriter(path.toFile())){
            fileWriter.write(schema.toString(true));
        }

        // create a file of ReflectedCustomers
        try {
            System.out.println("Writing customer-reflected.avro");
            File file = new File("customer-reflected.avro");
            DatumWriter<ReflectedCustomer> writer = new ReflectDatumWriter<>(ReflectedCustomer.class);
            DataFileWriter<ReflectedCustomer> out = new DataFileWriter<>(writer)
                    .setCodec(CodecFactory.deflateCodec(9))
                    .create(schema, file);

            out.append(new ReflectedCustomer("Bill", "Clark", "The Rocket"));
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // read from an avro into our Reflected class
        // open a file of ReflectedCustomers
        try {
            System.out.println("Reading customer-reflected.avro");
            File file = new File("customer-reflected.avro");
            DatumReader<ReflectedCustomer> reader = new ReflectDatumReader<>(ReflectedCustomer.class);
            DataFileReader<ReflectedCustomer> in = new DataFileReader<>(file, reader);

            // read ReflectedCustomers from the file & print them as JSON
            for (ReflectedCustomer reflectedCustomer : in) {
                System.out.println(reflectedCustomer.fullName());
            }
            // close the input file
            in.close();
        } catch (IOException e) {
            logger.info(e.getMessage());
        }



    }
}
