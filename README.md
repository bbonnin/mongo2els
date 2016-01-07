## Mongo2Els

Tool for copying data from MongoDB to Elasticsearch. There are two modes:
* __Bulk indexing__: the data are read from MongoDB (using a find query) and a bulk indexing is done. In this mode, a query and a projection can be provided, to select the data to write in Elasticsearch.
* __Real-time indexing__: the tool is tailing the MongoDB oplog and the data are indexing on-the-fly (when you insert a document in MongoDB, it is automatically indexed in Elasticsearch).

> **Important** : for real-time indexing, only `insert` and `delete` operations are supported.
> For `delete`, if you don't use the MongoDB _id to identify your document in Elasticsearch, the `delete`operation is not supported,
 because Elasticsearch does not support anymore `delete-by-query`.


### Build

Requirements:
* Maven 3
* Java 7

```bash
mvn clean package
```

### Run

#### Configuration

You have to provide a properties file to the application (you can reuse the `mongo2els-example.properties` file).

The supported properties are:


Property                   | Default value              | Description
-------------------------- | -------------------------- | ----------------------------------------------------------------------
mongo.host                 | localhost                  | mongod host
mongo.port                 | 27017                      | mongod port
mongo.db                   | test                       | Database name
mongo.collection           | test                       | Collection name
mongo.query                | {}                         | Query to select data (bulk mode only)
mongo.projection           | (none)                     | Projection used to select fields that will be indexed (bulk mode only)
mongo.batch.size           | 20                         | Batch size for the find query
elasticsearch.host         | localhost                  | Host or IP address
elasticsearch.port         | 9300                       | Transport port (**NOT HTTP**)
elasticsearch.index        | (same as mongo.db)         | Index name
elasticsearch.type         | (same as mongo.collection) | Document type name
elasticsearch.bulk.size    | 1000                       | Size of the bulk request (bulk mode only)
elasticsearch.bulk.threads | 1                          | Number of threads doing the bulk indexing
elasticsearch.use_mongo_id | true                       | The MongoDB _id will be used as identifier in Elasticsearch



#### Launch the application

* Bulk mode:
```bash
java -jar mongo2els-jar-with-dependencies.jar -m bulk -c mongo2els.properties
```

* Real-time mode:
```bash
java -jar mongo2els-jar-with-dependencies.jar -m realtime -c mongo2els.properties
```


### Test env

This tool has been tested with:
* Elasticsearch 2.1.0
* MongoDB 3.0.7, 3.2.0

