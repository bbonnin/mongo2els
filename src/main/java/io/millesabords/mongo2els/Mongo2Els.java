package io.millesabords.mongo2els;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Map;

/**
 * Simple tool for exporting data from MongoDB to Elasticsearch.
 *
 * @author Bruno Bonnin
 */
public class Mongo2Els {

    private static Logger LOGGER = LoggerFactory.getLogger(Mongo2Els.class);

    private static int BULK_SIZE = 1000;

    @Option(name = "-m", usage = "MongoDB connection URL (host:port)")
    private String mongodbUrl = "localhost:27017";

    @Option(name = "-d", usage = "MongoDB database")
    private String dbName = "test";

    @Option(name = "-c", usage = "MongoDB collection")
    private String collectionName = "test";

    @Option(name = "-q", usage = "MongoDB query")
    private String query = "{}";

    @Option(name = "-p", usage = "MongoDB query projection")
    private String projection = null;

    @Option(name = "-e", usage = "Elasticsearch connection URL (host:port)")
    private String elsUrl = "localhost:9300";

    @Option(name = "-i", usage = "Elasticsearch index (by default = db name)")
    private String index = null;

    @Option(name = "-t", usage = "Elasticsearch document type (by default = collection name)")
    private String doctype = null;

    @Option(name = "-b", usage = "Elasticsearch bulk request size")
    private int bulkSize = BULK_SIZE;

    private MongoClient mongoClient;

    private Jongo jongo;

    private Client elsClient;

    private int total = 0;


    public static void main(String[] args) {
        new Mongo2Els().doMain(args);
    }

    private void doMain(String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);

            if (index == null) {
                index = dbName;
            }
            if (doctype == null) {
                doctype = collectionName;
            }

            LOGGER.info("Config: {}", this);

            initMongoClient();
            initElsClient();

            LOGGER.info("Start : {}", new Date());

            transferData();
        }
        catch (CmdLineException e) {
            System.err.println("Bad argument");
            parser.printUsage(System.err);
        }
        catch (Exception e) {
            LOGGER.error("Problem during processing", e);
        }

        LOGGER.info("End : {}", new Date());
        LOGGER.info("Total of indexed docs : {}", total);
    }

    private void initMongoClient() {
        mongoClient = new MongoClient(new MongoClientURI("mongodb://" + mongodbUrl));
        mongoClient.getServerAddressList(); // To check that the connection is ok
        jongo = new Jongo(mongoClient.getDB(dbName));
    }

    private void initElsClient() throws IOException {
        final String host = elsUrl.substring(0, elsUrl.indexOf(":"));
        final int port = Integer.parseInt(elsUrl.substring(elsUrl.indexOf(":") + 1));

        final Settings settings = Settings.settingsBuilder()
            .put("client.transport.ignore_cluster_name", true).build();

        elsClient = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    }

    /**
     * Data transfer from MongoDB to Elasticsearch.
     * It uses a bulk indexation.
     */
    private void transferData() {
        final MongoCollection collection = jongo.getCollection(collectionName);
        final MongoCursor<Map> cursor = collection.find(query).projection(projection).as(Map.class);
        Map doc;
        BulkResponse bulkResponse;
        String id;
        int count = 0;
        BulkRequestBuilder bulkRequest = elsClient.prepareBulk();

        while (cursor.hasNext()) {
            total++;
            count++;
            doc = cursor.next();
            id = doc.remove("_id").toString();

            bulkRequest.add(elsClient.prepareIndex(index, doctype, id).setSource(doc));
            if (count % bulkSize == 0 || !cursor.hasNext()) {
                LOGGER.info("Indexing {} docs...", count);
                bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    LOGGER.error("ERROR : problems occur with the bulk request: {}", bulkResponse.buildFailureMessage());
                }
                LOGGER.info("Current total : {}", total);
                count = 0;
                bulkRequest = elsClient.prepareBulk();
            }

        }

    }

    @Override
    public String toString() {
        return "Mongo2Els:\n" +
                "  MongoDB url='" + mongodbUrl + "'\n" +
                "  MongoDB database name='" + dbName + "'\n" +
                "  MongoDB collection name='" + collectionName + "'\n" +
                "  MongoDB query='" + query + "'\n" +
                "  MongoDB projection='" + projection + "'\n" +
                "  Elasticsearch url='" + elsUrl + "'\n" +
                "  Elasticsearch index='" + index + "'\n" +
                "  Elasticsearch doc type='" + doctype + "'\n" +
                "  Elasticsearch bulk size='" + bulkSize + "'\n";
    }
}
