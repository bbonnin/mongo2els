package io.millesabords.mongo2els;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.jongo.Jongo;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Simple tool for exporting data from MongoDB to Elasticsearch.
 *
 * @author Bruno Bonnin
 */
public class Mongo2Els {

    private static Logger LOGGER = LoggerFactory.getLogger(Mongo2Els.class);

    @Option(name = "-m", usage = "Processing mode", required = true, metaVar = "<bulk | realtime>")
    private String mode;

    @Option(name = "-c", usage = "Configuration file", required = true, metaVar = "<file name>")
    private String configFileName;

    private MongoClient mongoClient;

    private Jongo jongo;

    private Client elsClient;

    public static void main(final String[] args) {
        new Mongo2Els().doMain(args);
    }

    private void doMain(final String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);

            final Config cfg = Config.get();
            cfg.load(new FileInputStream(configFileName));

            LOGGER.info("Config : {}", cfg);

            initMongoClient();
            initElsClient();

            LOGGER.info("Start : {}", new Date());

            if ("bulk".equalsIgnoreCase(mode)) {
                new BulkIndexing(jongo, elsClient, cfg.getInt(Config.ELS_BULK_THREADS)).indexData();
            }
            else if ("realtime".equalsIgnoreCase(mode)) {
                final RealtimeIndexing indexing = new RealtimeIndexing(elsClient);
                final OplogTailer tailer = new OplogTailer(mongoClient);
                tailer.addListener(indexing);
                final ExecutorService exec = Executors.newSingleThreadExecutor();
                exec.submit(tailer);
            }
            else {
                System.err.println("Bad mode : " + mode);
                parser.printUsage(System.err);
            }
        }
        catch (final CmdLineException e) {
            System.err.println("Bad argument");
            parser.printUsage(System.err);
        }
        catch (final Exception e) {
            LOGGER.error("Problem during processing", e);
        }

        LOGGER.info("End : {}", new Date());

    }

    @SuppressWarnings("deprecation")
    private void initMongoClient() {
        final Config cfg = Config.get();
        mongoClient = new MongoClient(new MongoClientURI(
            "mongodb://" + cfg.get(Config.MONGO_HOST) + ":" + cfg.get(Config.MONGO_PORT)));
        mongoClient.getServerAddressList(); // To check that the connection is ok
        jongo = new Jongo(mongoClient.getDB(cfg.get(Config.MONGO_DB)));
    }

    private void initElsClient() throws IOException {
        final Config cfg = Config.get();
        final String host = cfg.get(Config.ELS_HOST);
        final int port = cfg.getInt(Config.ELS_PORT);

        final Settings settings = Settings.settingsBuilder()
            .put("client.transport.ignore_cluster_name", true).build();

        elsClient = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }



}
