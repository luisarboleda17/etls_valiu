
import datetime
from marshmallow import Schema, fields, EXCLUDE, validate, pre_load

users_table_schema = {
    'fields': [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
    ]
}
users_table_partitioning = {
    'timePartitioning': {'type': 'MONTH'}
}


class UserSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def __parse_from_date_object__(self, date):
        if isinstance(date, datetime.date):
            return datetime.datetime.combine(date, datetime.datetime.min.time()).isoformat()
        elif isinstance(date, datetime.datetime):
            return date.isoformat()
        else:
            return date

    @pre_load()
    def parse_dates_object(self, data, many, **kwargs):
        return {
            **data,
            'created_at': self.__parse_from_date_object__(data['created_at'])
        }

    id = fields.Str(required=True)
    first_name = fields.Str(required=False, allow_none=True, missing=None)
    created_at = fields.DateTime(required=False, allow_none=True, missing=None)
