from pydantic import BaseModel, Field, root_validator

DataDict = dict[str, object]


class Coordinate(BaseModel):
    lat: float = Field(None)
    lon: float = Field(None)


class PoiData(BaseModel):
    name: str = Field(None)
    data_id: str = Field(None)
    id: str = Field(None)
    subcategory: str = Field(None)
    category: str = Field(None)
    address: str = Field(None)
    source: str = Field(None)
    keyword: str = Field(None)
    phone: str = Field(None)
    database: str = Field(None)
    index: str = Field(None)
    id_elastic: str = Field(None)
    location_name: str = Field(None)
    location_code: str = Field(None)
    location_coordinate: Coordinate = Field(None)
    country_name: str = Field(None)
    country_code: str = Field(None)
    country_coordinate: Coordinate = Field(None)
    province_name: str = Field(None)
    province_coordinate: Coordinate = Field(None)
    city_name: str = Field(None)
    city_coordinate: Coordinate = Field(None)
    district_name: str = Field(None)
    district_coordinate: Coordinate = Field(None)
    sub_district_name: str = Field(None)
    sub_district_coordinate: Coordinate = Field(None)

    @root_validator(pre=True)
    def coordinate_validator(cls, values: DataDict):
        if 'location_coordinate' in values:
            obj = Coordinate.parse_obj(values.get('location_coordinate'))
            values['location_coordinate'] = obj.dict()
        if 'country_coordinate' in values:
            obj = Coordinate.parse_obj(values.get('country_coordinate'))
            values['country_coordinate'] = obj.dict()
        if 'province_coordinate' in values:
            obj = Coordinate.parse_obj(values.get('province_coordinate'))
            values['province_coordinate'] = obj.dict()
        if 'district_coordinate' in values:
            obj = Coordinate.parse_obj(values.get('district_coordinate'))
            values['district_coordinate'] = obj.dict()
        if 'city_coordinate' in values:
            obj = Coordinate.parse_obj(values.get('city_coordinate'))
            values['city_coordinate'] = obj.dict()
        if 'sub_district_coordinate' in values:
            obj = Coordinate.parse_obj(values.get('sub_district_coordinate'))
            values['sub_district_coordinate'] = obj.dict()
