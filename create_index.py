from elasticsearch import Elasticsearch
import json

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def create_index(es_object, index_name='videos'):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "members": {
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "text"
                    },
                    "name": {
                        "type": "text"
                    },
                    "views": {
                        "type": "integer"
                    },
                    "likes": {
                        "type": "integer"
                    },
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(record):
    elastic_object = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    is_stored = True
    try:
        outcome = elastic_object.index(index='videos', doc_type='members', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def search(es_object, search):
    res = es_object.search(index="videos", body=search)
    print(res)

#res = es.get(index="videos", id='sEKbPnMB89eVZvljTlm-')
#print(res['_source'])


res = es.search(index="videos", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    print(hit["_id"], (" : %(name)s %(views)i %(likes)i" % hit["_source"]))
