import time

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
    if key in POINT and key is not None:
        return "ST_GeomFromText(%s, 4326)"
    return "%s"


def check_anomalies_values(key: str, value):
    if value == '':
        return None
    if isinstance(value, dict):
        if value['lon'] is None or value['lat'] is None:
            return None
    return key


def geom_parser(key: str, value):
    if key in POINT and value is not None:
        return f"POINT({value['lon']} {value['lat']})"
    return value


def check_anomalies_parser(key: str, value):
    if value == '':
        return None
    if isinstance(value, dict):
        if value['lon'] is None or value['lat'] is None:
            return None
    return value


def insert_pg(bulk_data: list):
    config = PostgresConfig()
    connect = config.connect()
    cursor = connect.cursor()

    data_key = bulk_data[0]
    query_insert = f"""INSERT INTO production_point_of_interest_2 ({','.join([key for key, value in data_key.items()])})
                VALUES"""
    first_values = f"({','.join([geom_point(check_anomalies_values(key, value)) for key, value in data_key.items()])})"
    first_param = tuple([geom_parser(key, check_anomalies_parser(key, value)) for key, value in data_key.items()])
    for data in bulk_data[1:]:
        next_values = f"({','.join([geom_point(check_anomalies_values(key, value)) for key, value in data.items()])})"
        next_params = tuple([geom_parser(key, check_anomalies_parser(key, value)) for key, value in data.items()])

        first_values = f"{first_values},{next_values}"
        first_param = first_param + next_params
    query_insert = f"{query_insert}{first_values}"

    # print(query_insert)
    # print(first_param)
    cursor.execute(query_insert, first_param)
    connect.commit()
    cursor.close()
    connect.close()


count = 0
if __name__ == "__main__":
    with Elasticsearch(
            hosts="http://10.12.1.51:5200",
            http_auth=("ingest_ai", "1ngest4i2o23"),
            timeout=3600
    ) as es:
        bulk_data = []
        start_time = time.time()
        for hit in scan(client=es, index="production-point-of-interest", query=query(), size=1000, request_timeout=3600,
                        scroll='10m'):
            source = hit['_source']

            payload: dict = source
            location = {}
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
            bulk_data.append(payload)
            # print(payload)
            if len(bulk_data) == 1000:
                count += 1000
                insert_pg(bulk_data)
                bulk_data.clear()

                finish_time = time.time() - start_time
                print("=======================")
                print(f"Already ingest {count}/1605651")
                print(f"Execute time: {finish_time}")
                print("=======================")
                start_time = time.time()
        if len(bulk_data):
            insert_pg(bulk_data)
            bulk_data.clear()
            count += len(bulk_data)
            finish_time = time.time() - start_time
            print("=======================")
            print(f"Already ingest {count}/1605651")
            print(f"Execute time: {finish_time}")
            print("=======================")
            start_time = time.time()
