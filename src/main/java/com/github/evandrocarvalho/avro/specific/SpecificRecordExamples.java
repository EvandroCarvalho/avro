package com.github.evandrocarvalho.avro.specific;

import com.example.Customer;
import com.github.evandrocarvalho.avro.generic.GenericRecordExamples;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author Evandro Carvalho
 */
public class SpecificRecordExamples {
    final static Logger logger = LoggerFactory.getLogger(GenericRecordExamples.class);

    public static void main(String[] args) {
        // step 1: create specific record
        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("Evandro");
        customerBuilder.setLastName("Junior");
        customerBuilder.setHeight(175.5f);
        customerBuilder.setWeight(80.5f);
        customerBuilder.setAutomatedEmail(false);
        final Customer customer = customerBuilder.build();

        System.out.println(customer);

        // step 2: write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written customer-specific.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            logger.info(e.getMessage());
        }

        // step 3: read from file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>();
        final DataFileReader<Customer> dataFileReader;
        try {
            // step4 interpret as a specific record
            System.out.println("Reading our specific record");
            dataFileReader = new DataFileReader<Customer>(file, datumReader);
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("First name: " + readCustomer.getFirstName());
            }
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }
}
