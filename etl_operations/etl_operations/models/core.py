
import datetime
from marshmallow import Schema, fields, EXCLUDE, validate, pre_load

transactions_table_schema = {
    'fields': [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'id2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'account_id_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'account_id_src', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'amount_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'amount_dst_usd', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'amount_src', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'amount_src_usd', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'asset_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'asset_src', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contact_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'DATETIME', 'mode': 'REQUIRED'},
        {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'service_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'short_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sync_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'user_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'v', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'wallet_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'wallet_src', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transaction_type', 'type': 'STRING', 'mode': 'REQUIRED'},
    ]
}
transactions_table_partitioning = {
    'timePartitioning': {'type': 'DAY'}
}


class TransactionSchema(Schema):
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
            'created_at': self.__parse_from_date_object__(data['created_at']),
            'updated_at': self.__parse_from_date_object__(data['updated_at']),
            'sync_date': self.__parse_from_date_object__(data['sync_date'])
        }

    id = fields.Str(required=True)
    id2 = fields.Str(required=False, allow_none=True, missing=None)
    account_id_dst = fields.Str(required=False, allow_none=True, missing=None)
    account_id_src = fields.Str(required=False, allow_none=True, missing=None)
    amount_dst = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_dst_usd = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_src = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_src_usd = fields.Decimal(required=False, allow_none=True, missing=None)
    asset_dst = fields.Str(required=False, allow_none=True, missing=None)
    asset_src = fields.Str(required=False, allow_none=True, missing=None)
    contact_dst = fields.Str(required=False, allow_none=True, missing=None)
    created_at = fields.DateTime(required=True)
    description = fields.Str(required=False, allow_none=True, missing=None)
    order_id = fields.Str(required=False, allow_none=True, missing=None)
    service_name = fields.Str(required=False, allow_none=True, missing=None)
    short_id = fields.Str(required=False, allow_none=True, missing=None)
    state = fields.Str(required=False, allow_none=True, missing=None)
    sync_date = fields.DateTime(required=False, allow_none=True, missing=None)
    type = fields.Str(required=False, allow_none=True, missing=None)
    updated_at = fields.DateTime(required=False, allow_none=True, missing=None)
    user_id = fields.Str(required=True)
    user_type = fields.Str(required=False, allow_none=True, missing=None)
    v = fields.Str(required=False, allow_none=True, missing=None)
    wallet_dst = fields.Str(required=False, allow_none=True, missing=None)
    wallet_src = fields.Str(required=False, allow_none=True, missing=None)
    transaction_type = fields.Str(
        required=True,
        validate=validate.OneOf(["cash_in", "cash_out", "p2p"])
    )
