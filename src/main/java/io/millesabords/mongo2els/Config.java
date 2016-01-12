package io.millesabords.mongo2els;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration of the application.
 *
 * @author Bruno Bonnin
 */
public class Config extends Properties {

    private static final long serialVersionUID = -7269241682972794667L;

    private static Config INSTANCE = new Config();

    public static final String MONGO_HOST = "mongo.host";
    public static final String MONGO_PORT = "mongo.port";
    public static final String MONGO_DB = "mongo.db";
    public static final String MONGO_COLLECTION = "mongo.collection";
    public static final String MONGO_QUERY = "mongo.query";
    public static final String MONGO_PROJECTION = "mongo.projection";
    public static final String MONGO_BATCH_SIZE = "mongo.batch.size";

    public static final String ELS_HOST = "elasticsearch.host";
    public static final String ELS_PORT = "elasticsearch.port";
    public static final String ELS_INDEX = "elasticsearch.index";
    public static final String ELS_TYPE = "elasticsearch.type";
    public static final String ELS_BULK_SIZE = "elasticsearch.bulk.size";
    public static final String ELS_BULK_THREADS = "elasticsearch.bulk.threads";
    public static final String ELS_USE_MONGO_ID = "elasticsearch.use_mongo_id";
    public static final String ELS_MONGO_ID_NAME = "elasticsearch.mongo_id_name";

    public static Config get() {
        return INSTANCE;
    }

    private Config() {
        put(MONGO_HOST, "localhost");
        put(MONGO_PORT, "27017");
        put(MONGO_DB, "test");
        put(MONGO_COLLECTION, "test");
        put(MONGO_QUERY, "{}");
        put(MONGO_BATCH_SIZE, "20");

        put(ELS_HOST, "localhost");
        put(ELS_PORT, "9300");
        put(ELS_BULK_SIZE, "1000");
        put(ELS_BULK_THREADS, "1");
        put(ELS_USE_MONGO_ID, "true");
        put(ELS_MONGO_ID_NAME, "_mongo_id");
    }

    public String get(final String key) {
        return getProperty(key);
    }

    public int getInt(final String key) {
        return Integer.parseInt(get(key));
    }

    public boolean getBoolean(final String key) {
        return Boolean.parseBoolean(get(key));
    }

    @Override
    public synchronized void load(final InputStream is) throws IOException {
        super.load(is);

        if (get(ELS_INDEX) == null || get(ELS_INDEX).trim().length() == 0) {
            put(ELS_INDEX, get(MONGO_DB));
        }
        if (get(ELS_TYPE) == null || get(ELS_TYPE).trim().length() == 0) {
            put(ELS_TYPE, get(MONGO_COLLECTION));
        }
    }

    @Override
    public String toString() {
        return "Config: " + super.toString();
    }
}
