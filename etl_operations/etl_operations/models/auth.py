
from marshmallow import Schema, fields, EXCLUDE
from etl_operations.models.serializer import AutoParsedDate

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

    id = fields.Str(required=True)
    first_name = fields.Str(required=False, allow_none=True, missing=None)
    created_at = AutoParsedDate(required=False, allow_none=True, missing=None)
