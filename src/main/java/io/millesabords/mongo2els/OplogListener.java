package io.millesabords.mongo2els;

import org.bson.Document;

/**
 * Listener for oplog updates.
 * 
 * @author Bruno Bonnin
 */
public interface OplogListener {
    
    void onOplog(Document doc);
}
