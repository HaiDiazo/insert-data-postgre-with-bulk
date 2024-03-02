from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

config = {
    "ES_HOST": "http://192.168.24.140:5200"
}

query = {
    "_source": {
        "excludes": ["geometry_city", "geometry_province", "coordinate_city", "coordinate_province"]
    },
    "query": {
        "match_all": {}
    }
}


def fetch():
    with Elasticsearch(
            hosts=config.get('ES_HOST')
    ) as es:
        for hit in scan(client=es, query=query, index='gas-konflik-agraria', size=100):
            source = hit['_source']
            if 'location' in source:
                location = source['location'].split(', ')
                source['location'] = {
                    'lat': float(location[0]),
                    'lon': float(location[1])
                }
            yield {
                '_index': 'gas-konflik-agraria-mapping',
                '_id': hit['_id'],
                '_source': source
            }


def run():
    with Elasticsearch(
            hosts=config.get('ES_HOST')
    ) as es:
        for ok, info in streaming_bulk(client=es, actions=fetch(), chunk_size=100):
            if not ok:
                raise Exception(info)
            print(info)


run()
