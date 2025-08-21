from elasticsearch import Elasticsearch, helpers

class ESLoader:
    def __init__(self, index: str, es_url: str = "http://localhost:9200"):
        self.es = Elasticsearch(es_url)
        self.index = index

    def load(self, docs: list[dict]):
        actions = [
            {
                "_index": self.index,
                "_id": doc["id"],
                "_op_type": "update",
                "doc": doc,
                "doc_as_upsert": True,
            }
            for doc in docs
        ]
        helpers.bulk(self.es, actions)