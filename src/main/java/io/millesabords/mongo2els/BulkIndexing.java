package io.millesabords.mongo2els;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.jongo.QueryModifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCursor;

/**
 * Bulk indexing of data.
 *
 * @author Bruno Bonnin
 */
public class BulkIndexing {

    private static Logger LOGGER = LoggerFactory.getLogger(BulkIndexing.class);

    private final ExecutorService exec;

    private final Jongo jongo;

    private final Client elsClient;

    public BulkIndexing(final Jongo jongo, final Client elsClient, final int nbThreads) {
        this.jongo = jongo;
        this.elsClient = elsClient;
        this.exec = Executors.newFixedThreadPool(nbThreads);
    }

    /**
     * Data transfer from MongoDB to Elasticsearch using a bulk indexing.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void indexData() {
        final Config cfg = Config.get();

        final MongoCollection collection = jongo.getCollection(cfg.get(Config.MONGO_COLLECTION));
        final MongoCursor<Map> cursor = collection
                .find(cfg.get(Config.MONGO_QUERY)).projection(cfg.get(Config.MONGO_PROJECTION))
                .with(new QueryModifier() {
                    @Override
                    public void modify(DBCursor cursor) {
                        cursor.batchSize(cfg.getInt(Config.MONGO_BATCH_SIZE));
                    }
                })
                .as(Map.class);
        final String elsIndex = cfg.get(Config.ELS_INDEX);
        final String elsType = cfg.get(Config.ELS_TYPE);
        final int bulkSize = cfg.getInt(Config.ELS_BULK_SIZE);

        Map doc;

        String id;
        int total = 0;
        int count = 0;
        BulkRequestBuilder bulkRequest = elsClient.prepareBulk();

        while (cursor.hasNext()) {
            total++;
            count++;
            doc = cursor.next();
            id = doc.remove("_id").toString();
            if (cfg.getBoolean(Config.ELS_USE_MONGO_ID)) {
                bulkRequest.add(elsClient.prepareIndex(elsIndex, elsType, id).setSource(doc));
            }
            else {
                doc.put("_mongo_id", id);
                bulkRequest.add(elsClient.prepareIndex(elsIndex, elsType).setSource(doc));
            }
            if (count % bulkSize == 0 || !cursor.hasNext()) {
                LOGGER.info("Current total : {}", total);
                exec.submit(new BulkIndexingTask(bulkRequest, count));
                count = 0;
                bulkRequest = elsClient.prepareBulk();
            }
        }

        exec.shutdown();

        LOGGER.info("Total of indexed docs : {}", total);
    }

    class BulkIndexingTask implements Callable<Void> {

        private final BulkRequestBuilder bulkRequest;
        private final int nbDocs;

        public BulkIndexingTask(final BulkRequestBuilder bulkRequest, final int nbDocs) {
            this.bulkRequest = bulkRequest;
            this.nbDocs = nbDocs;
        }

        @Override
        public Void call() throws Exception {
            LOGGER.info("Indexing {} docs...", nbDocs);
            final BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                LOGGER.error("ERROR : problems occur with the bulk request: {}", bulkResponse.buildFailureMessage());
            }
            return null;
        }

    }
}
