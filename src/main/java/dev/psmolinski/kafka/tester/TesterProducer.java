package dev.psmolinski.kafka.tester;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TesterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TesterProducer.class);

    private String topic;

    private Long interval;

    private Properties config;

    public void execute() throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                config,
                new StringSerializer(),
                new StringSerializer())) {

            for (;;) {

                long t0 = System.currentTimeMillis();
                long t1;

                String key = UUID.randomUUID().toString();
                String message = sdf.format(new Date(t0));

                Future<RecordMetadata> future =  producer.send(
                        new ProducerRecord<String, String>(
                                topic,
                                null,
                                t0,
                                key,
                                message,
                                null
                        )
                );

                producer.flush();

                try {

                    RecordMetadata m = future.get();

                    t1 = System.currentTimeMillis();

                    logger.info(
                            "Send SUCCESS, key: {}, latency: {}, partition: {}, offset: {}",
                            key,
                            t1-t0,
                            m.partition(),
                            m.offset()
                    );

                } catch (ExecutionException e) {

                    t1 = System.currentTimeMillis();

                    logger.info("Send FAILURE, key: {}, latency: {}", key, t1-t0);

                    logger.warn("Failure details", e.getCause());

                }

                long remaining = interval - (t1-t0);

                if (remaining>0) {
                    Thread.sleep(remaining);
                }

            }

        }
    }

    public static void main(String...args) throws Exception {

        TesterProducer producer = buildTesterProducer(args);

        try {
            producer.execute();
        } catch (Throwable e) {
            logger.error("Producer loop failed", e);
            System.exit(1);
        }

    }

    // ---- private ----

    private static TesterProducer buildTesterProducer(String...args) throws Exception {

        TesterProducer testerProducer = new TesterProducer();
        for (int i=0;i<args.length;i++) {
            if ("--topic".equals(args[i])) {
                if (i+1>= args.length) {
                    System.err.println("--topic requires argument");
                    System.exit(1);
                }
                if (testerProducer.topic!=null) {
                    System.err.println("duplicate --topic option");
                    System.exit(1);
                }
                testerProducer.topic = args[++i];
                continue;
            }
            if ("--config".equals(args[i])) {
                if (i+1>= args.length) {
                    System.err.println("--config requires argument");
                    System.exit(1);
                }
                if (testerProducer.config!=null) {
                    System.err.println("duplicate --config option");
                    System.exit(1);
                }
                try {
                    testerProducer.config = readConfig(args[++i]);
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
                if (testerProducer.interval!=null) {
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
                testerProducer.interval = interval;
                continue;
            }
            System.err.println("Unknown option: "+args[i]);
            System.exit(1);
        }

        if (testerProducer.topic==null) {
            testerProducer.topic = System.getenv("TESTER_TOPIC");
        }
        if (testerProducer.topic==null) {
            testerProducer.topic = "test";
        }
        if (testerProducer.config==null) {
            String config = System.getenv("TESTER_CONFIG");
            if (config==null) config = getDefaultConfig();
            try {
                testerProducer.config = readConfig(config);
            } catch (Exception e) {
                System.err.println("Could not read config: "+config);
                System.exit(1);
            }
        }
        if (testerProducer.interval==null) {
            testerProducer.interval = 1000L;
        }

        return testerProducer;

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
