from config.pg_config import PostgresConfig
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

POINT = [
    'location_coordinate',
    'province_coordinate',
    'country_coordinate',
    'city_coordinate',
    'district_coordinate',
    'sub_district_coordinate'
]


def query():
    return {
        "sort": [
            {
                "id_elastic.keyword": {
                    "order": "asc"
                }
            }
        ]
    }


def geom_point(key: str):
    if key in POINT:
        return "ST_GeomFromText(%s, 4326)"
    return "%s"


def geom_parser(key: str, value):
    if key in POINT:
        return f"POINT({value['lon']} {value['lat']})"
    return value


def check_anomalies(key, value):
    if value == '':
        return False
    if isinstance(value, dict):
        if value['lon'] is None or value['lat'] is None:
            return False
    return True


def insert_pg(data: dict):
    config = PostgresConfig()
    connect = config.connect()
    cursor = connect.cursor()

    query_insert = f"""INSERT INTO production_point_of_interest ({','.join([key for key, value in data.items() if check_anomalies(key, value)])})
                VALUES({','.join([geom_point(key) for key, value in data.items() if check_anomalies(key, value)])})
            """
    print(query_insert)
    param = tuple([geom_parser(key, value) for key, value in data.items() if check_anomalies(key, value)])
    print(param)
    cursor.execute(query_insert, param)
    connect.commit()
    connect.close()


if __name__ == "__main__":
    with Elasticsearch(
            hosts="http://10.12.1.51:5200",
            http_auth=("ingest_ai", "1ngest4i2o23"),
            timeout=3600
    ) as es:
        for hit in scan(client=es, index="production-point-of-interest", query=query(), size=100, request_timeout=3600,
                        scroll='10m'):
            source = hit['_source']

            payload: dict = source
            if 'location' in source and source['location'] is not None:
                location = source['location']
                location_payload = {
                    'location_name': location.get('location_name'),
                    'location_code': location.get('location_code'),
                    'location_coordinate': location.get('location_coordinate'),
                    'country_name': location.get('country_name'),
                    'country_code': location.get('country_code'),
                    'country_coordinate': location.get('country_coordinate'),
                    'province_name': location.get('province_name'),
                    'province_coordinate': location.get('province_coordinate'),
                    'city_name': location.get('city_name'),
                    'city_coordinate': location.get('city_coordinate'),
                    'district_name': location.get('district_name'),
                    'district_coordinate': location.get('district_coordinate'),
                    'sub_district_name': location.get('sub_district_name'),
                    'sub_district_coordinate': location.get('sub_district_coordinate'),
                }
                del payload['location']
                payload.update(location_payload)
            print(payload)
            insert_pg(payload)
