package io.millesabords.mongo2els;

import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk indexing of data.
 * 
 * @author Bruno Bonnin
 */
public class BulkIndexing {
    
    private static Logger LOGGER = LoggerFactory.getLogger(BulkIndexing.class);
    
    private final Jongo jongo;

    private final Client elsClient;
    
    public BulkIndexing(Jongo jongo, Client elsClient) {
        this.jongo = jongo;
        this.elsClient = elsClient;
    }

    /**
     * Data transfer from MongoDB to Elasticsearch using a bulk indexing.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void indexData() {
        final Config cfg = Config.get();
        
        final MongoCollection collection = jongo.getCollection(cfg.get(Config.MONGO_COLLECTION));
        final MongoCursor<Map> cursor = collection
            .find(cfg.get(Config.MONGO_QUERY)).projection(cfg.get(Config.MONGO_PROJECTION)).as(Map.class);
        final String elsIndex = cfg.get(Config.ELS_INDEX);
        final String elsType = cfg.get(Config.ELS_TYPE);
        final int bulkSize = cfg.getInt(Config.ELS_BULK_SIZE);

        Map doc;
        BulkResponse bulkResponse;
        String id;
        int total = 0;
        int count = 0;
        BulkRequestBuilder bulkRequest = elsClient.prepareBulk();

        while (cursor.hasNext()) {
            total++;
            count++;
            doc = cursor.next();
            id = doc.remove("_id").toString();
            bulkRequest.add(elsClient.prepareIndex(elsIndex, elsType, id).setSource(doc));
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
        
        LOGGER.info("Total of indexed docs : {}", total);
    }
}
