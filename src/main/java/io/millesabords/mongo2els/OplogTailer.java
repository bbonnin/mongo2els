package io.millesabords.mongo2els;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.types.BSONTimestamp;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * MongoDB oplog tailer.
 * 
 * @author Bruno Bonnin
 */
public class OplogTailer implements Callable<Void> {
    
    private final MongoClient client;
    private final MongoCollection<Document> oplog;
    private final String ns;
    private final List<OplogListener> listeners = new LinkedList<>();
    private final Config cfg = Config.get();
    
    public OplogTailer(final MongoClient client) {
        this.client = client;
        this.ns = cfg.get(Config.MONGO_DB) + "." + cfg.get(Config.MONGO_COLLECTION);
        this.oplog = this.client.getDatabase("local").getCollection("oplog.rs");
    }

    @Override
    public Void call() throws Exception {
        
        final Date now = new Date();
        final Document query = new Document("ns", ns)
            .append("ts", new Document("$gt", new BSONTimestamp((int) (now.getTime() / 1000), 0)));
        
        final MongoCursor<Document> cursor = oplog.find(query)
            .cursorType(CursorType.TailableAwait).iterator();
        
        while (cursor.hasNext()) {
            final Document doc = cursor.next();
            for (final OplogListener listener : listeners) {
                listener.onOplog(doc);
            }
        }

        return null;
    }
    
    public void addListener(OplogListener listener) {
        listeners.add(listener);
    }
}
