package dev.psmolinski.kafka.tester;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

public class TesterConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TesterConsumer.class);

    private String topic;

    private Long interval;

    private Properties config;

    public void execute() throws Exception {

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                config,
                new StringDeserializer(),
                new StringDeserializer())) {

            consumer.subscribe(Collections.singleton(topic));

            for (;;) {

                long t0 = System.currentTimeMillis();
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(interval));
                long t1 = System.currentTimeMillis();

                logger.info("Polled {} records; latency {}", records.count(), t1-t0);

                for (ConsumerRecord<String,String> record : records) {

                    logger.info(
                            "Received record from partition {}; latency {}",
                            record.partition(),
                            t1-record.timestamp());

                }

            }

        }
    }

    public static void main(String...args) throws Exception {

        TesterConsumer consumer = buildTesterConsumer(args);

        try {
            consumer.execute();
        } catch (Throwable e) {
            logger.error("Consumer loop failed", e);
            System.exit(1);
        }

    }

    // ---- private ----

    private static TesterConsumer buildTesterConsumer(String...args) throws Exception {

        TesterConsumer testerConsumer = new TesterConsumer();
        for (int i=0;i<args.length;i++) {
            if ("--topic".equals(args[i])) {
                if (i+1>= args.length) {
                    System.err.println("--topic requires argument");
                    System.exit(1);
                }
                if (testerConsumer.topic!=null) {
                    System.err.println("duplicate --topic option");
                    System.exit(1);
                }
                testerConsumer.topic = args[++i];
                continue;
            }
            if ("--config".equals(args[i])) {
                if (i+1>= args.length) {
                    System.err.println("--config requires argument");
                    System.exit(1);
                }
                if (testerConsumer.config!=null) {
                    System.err.println("duplicate --config option");
                    System.exit(1);
                }
                try {
                    testerConsumer.config = readConfig(args[++i]);
                } catch (Exception e) {
                    System.err.println("Could not read config: "+args[i]);
                    System.exit(1);
                }
                continue;
            }
            if ("--interval".equals(args[i])) {
                if (i+1>= args.length) {
                    System.err.println("--interval requires argument");
                    System.exit(1);
                }
                if (testerConsumer.interval!=null) {
                    System.err.println("duplicate --interval option");
                    System.exit(1);
                }
                Long interval = 0L;
                try {
                    interval = new Long(args[++i]);
                } catch (Exception e) {
                    System.err.println("Invalid interval: "+args[i]);
                    System.exit(1);
                }
                if (interval<=0L) {
                    System.err.println("Invalid interval: "+args[i]);
                    System.exit(1);
                }
                testerConsumer.interval = interval;
                continue;
            }
            System.err.println("Unknown option: "+args[i]);
            System.exit(1);
        }

        if (testerConsumer.topic==null) {
            testerConsumer.topic = System.getenv("TESTER_TOPIC");
        }
        if (testerConsumer.topic==null) {
            testerConsumer.topic = "test";
        }
        if (testerConsumer.config==null) {
            String config = System.getenv("TESTER_CONFIG");
            if (config==null) config = getDefaultConfig();
            try {
                testerConsumer.config = readConfig(config);
            } catch (Exception e) {
                System.err.println("Could not read config: "+config);
                System.exit(1);
            }
        }
        if (testerConsumer.interval==null) {
            testerConsumer.interval = 1000L;
        }

        return testerConsumer;

    }

    private static Properties readConfig(String config) throws Exception {
        Properties props = new Properties();
        try (InputStream stream = new FileInputStream(config)) {
            props.load(stream);
        }
        return props;
    }

    private static String getDefaultConfig() throws Exception {
        String homeDir = System.getProperty("user.home");
        String configDir = new File(homeDir, ".config").getCanonicalPath();
        return new File(configDir, "tester.config").getCanonicalPath();
    }

}
