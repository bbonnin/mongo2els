## Mongo2Els

Tool for copying data from MongoDB to Elasticsearch.

### Build

Requirements:
* Maven 3
* Java 8

```bash
mvn clean package
```

### Run

Arguments:
* -m : MongoDB connection URL (host:port)
* -d : MongoDB database
* -c : MongoDB collection
* -q : MongoDB query (to select data)
* -p : MongoDB query projection (to select fields to be indexed in Elasticsearch)
* -e : Elasticsearch connection URL (host:port)
* -i : Elasticsearch index (by default = db name)
* -t : Elasticsearch document type (by default = collection name)

Run with the default arguments:

```bash

java -jar mongo2els-jar-with-dependencies.jar

2015-12-13 12:28:05.047 [main] INFO  io.millesabords.mongo2els.Mongo2Els - Config: Mongo2Els:
  MongoDB url='localhost:27017'
  MongoDB database name='test'
  MongoDB collection name='test'
  MongoDB query='{}'
  MongoDB projection='null'
  Elasticsearch url='localhost:9300'
  Elasticsearch index='test'
  Elasticsearch doc type='test'
  Elasticsearch bulk size='1000'

```


Run with arguments:

```bash

java -jar mongo2els-jar-with-dependencies.jar -m localhost:27100 -e localhost:10300 -d mydb \
                                              -c mycollection -p "{'fileName':1}" \
                                              -i myindex -t mytype -q "{'path':'/etc'}" -b 100

2015-12-13 12:32:46.531 [main] INFO  io.millesabords.mongo2els.Mongo2Els - Config: Mongo2Els:
  MongoDB url='localhost:27100'
  MongoDB database name='mydb'
  MongoDB collection name='mycollection'
  MongoDB query='{'path':'/etc'}'
  MongoDB projection='{'fileName':1}'
  Elasticsearch url='localhost:10300'
  Elasticsearch index='myindex'
  Elasticsearch doc type='mytype'
  Elasticsearch bulk size='100'

```

### Env

This tool has been tested with:
* Elasticsearch 2.1.0
* MongoDB 3.0.7

