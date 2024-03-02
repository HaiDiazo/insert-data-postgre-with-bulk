from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def query():
    return {
        "_source": [
            "title",
            "category",
            "location",
            "statements",
            "entities",
            "enc_5w1h"
        ],
        "query": {
            "range": {
                "created_time": {
                    "gte": "2010",
                    "lte": "2015",
                    "format": "yyyy"
                }
            }
        }
    }


if __name__ == "__main__":
    with Elasticsearch(
            hosts="http://10.12.1.51:5200",
            http_auth=("ingest_ai", "1ngest4i2o23"),
            timeout=3600
    ) as es:
        bulk_data = []
        for hit in scan(
                client=es,
                index="knowledge-reprocess-*",
                query=query(),
                size=100,
                request_timeout=3600,
                scroll='10m'
        ):
            source = hit['_source']
