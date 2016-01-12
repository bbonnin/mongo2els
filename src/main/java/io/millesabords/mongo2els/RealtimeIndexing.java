package io.millesabords.mongo2els;

import org.bson.Document;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming mode of Mongo2Els.
 *
 * @author Bruno Bonnin
 */
public class RealtimeIndexing implements OplogListener {

    private static Logger LOGGER = LoggerFactory.getLogger(RealtimeIndexing.class);

    private final Client elsClient;

    private final Config cfg = Config.get();

    public RealtimeIndexing(final Client elsClient) {
        this.elsClient = elsClient;
    }

    @Override
    public void onOplog(final Document oplog) {
        //TODO : use of the projection. For the moment, index the whole document
        final String elsIndex = cfg.get(Config.ELS_INDEX);
        final String elsType = cfg.get(Config.ELS_TYPE);
        final boolean useMongoId = cfg.getBoolean(Config.ELS_USE_MONGO_ID);

        final String operation = oplog.getString("op"); // operation : u=update, i=insert, d=delete

        if ("i".equalsIgnoreCase(operation)) {
            final Document obj = (Document) oplog.get("o");
            String id = null;

            if (!useMongoId) {
                obj.put(cfg.get(Config.ELS_MONGO_ID_NAME), obj.remove("_id"));
            }
            else {
                id = obj.remove("_id").toString();
            }

            try {
                elsClient
                    .prepareIndex(elsIndex, elsType, id)
                    .setSource(obj)
                    .get();
            }
            catch (final ElasticsearchException e) {
                LOGGER.error("Index doc=" + obj, e);
            }
        }
        else if ("d".equalsIgnoreCase(operation)) {
            final Document obj = (Document) oplog.get("o");

            try {
                if (useMongoId) {
                    elsClient
                        .prepareDelete(elsIndex, elsType, obj.get("_id").toString())
                        .get();
                }
                else {
                    LOGGER.warn("No more 'delete by query' in Elasticsearch");
                }
            }
            catch (final ElasticsearchException e) {
                LOGGER.error("Index doc=" + obj, e);
            }

        }
        else {
            // Ignore other operations
        }

    }

}
