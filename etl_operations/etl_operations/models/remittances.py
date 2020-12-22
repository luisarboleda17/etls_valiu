
from marshmallow import Schema, fields, EXCLUDE
from etl_operations.models.serializer import AutoParsedDate

remittances_table_schema = {
    'fields': [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'amount_after_cost_gateway', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'amount_after_fee', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'amount_dst', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'amount_fee', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'amount_total', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'amount_total_usd', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'btc_buy', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'btc_buy_cost', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'btc_profit', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'btc_profit_cost', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'btc_sell', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
        {'name': 'btc_sell_cost', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'callback_confirmation_request_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'callback_confirmation_request_body', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cost_gateway', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'created_by_user', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_via', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_via_app_version', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'currency_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'currency_src', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'deposit_status', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'fee', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'fee_fixed_gateway', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'fee_gateway', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'files', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gross_profit', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'history', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'is_sync', 'type': 'BOOL', 'mode': 'NULLABLE'},
        {'name': 'payment', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_method', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_methods', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_provider', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'payment_response', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'priority', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'profit_total', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'profit_total_usd', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'quote', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'quote_fixed', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'reactions', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'receipt_times', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'receipt_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'recipient', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'reference', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'remaining_dst', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'stocks', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'stocks_customer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'stocks_profit', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'tags', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'teller', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'updated_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'user_processor', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'v', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'v1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_by_user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_by_user_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'created_by_user_created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'teller_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'teller_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'teller_created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'user_processor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'user_processor_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'user_processor_created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
    ]
}
remittances_table_partitioning = {
    'timePartitioning': {'type': 'DAY'}
}


class RemittanceSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    id = fields.String(required=True)
    amount_after_cost_gateway = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_after_fee = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_dst = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_fee = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_total = fields.Decimal(required=False, allow_none=True, missing=None)
    amount_total_usd = fields.Decimal(required=False, allow_none=True, missing=None)
    btc_buy = fields.Decimal(required=False, allow_none=True, missing=None)
    btc_buy_cost = fields.Decimal(required=False, allow_none=True, missing=None)
    btc_profit = fields.Decimal(required=False, allow_none=True, missing=None)
    btc_profit_cost = fields.Decimal(required=False, allow_none=True, missing=None)
    btc_sell = fields.Decimal(required=False, allow_none=True, missing=None)
    btc_sell_cost = fields.Decimal(required=False, allow_none=True, missing=None)
    callback_confirmation_request_at = AutoParsedDate(required=False, allow_none=True, missing=None)
    callback_confirmation_request_body = fields.String(required=False, allow_none=True, missing=None)
    cost_gateway = fields.Decimal(required=False, allow_none=True, missing=None)
    created_at = AutoParsedDate(required=False, allow_none=True, missing=None)
    created_by_user = fields.String(required=False, allow_none=True, missing=None)
    created_via = fields.String(required=False, allow_none=True, missing=None)
    created_via_app_version = fields.String(required=False, allow_none=True, missing=None)  # MARK
    currency_dst = fields.String(required=False, allow_none=True, missing=None)
    currency_src = fields.String(required=False, allow_none=True, missing=None)
    customer = fields.String(required=False, allow_none=True, missing=None)
    customer_type = fields.String(required=False, allow_none=True, missing=None)
    deposit_status = fields.Boolean(required=False, allow_none=True, missing=None)
    fee = fields.Decimal(required=False, allow_none=True, missing=None)
    fee_fixed_gateway = fields.Decimal(required=False, allow_none=True, missing=None)
    fee_gateway = fields.Decimal(required=False, allow_none=True, missing=None)
    files = fields.String(required=False, allow_none=True, missing=None)
    gross_profit = fields.Decimal(required=False, allow_none=True, missing=None)
    history = fields.String(required=False, allow_none=True, missing=None)
    is_sync = fields.Boolean(required=False, allow_none=True, missing=None)
    payment = fields.String(required=False, allow_none=True, missing=None)
    payment_method = fields.String(required=False, allow_none=True, missing=None)
    payment_methods = fields.String(required=False, allow_none=True, missing=None)
    payment_provider = fields.String(required=False, allow_none=True, missing=None)
    payment_response = fields.String(required=False, allow_none=True, missing=None)
    priority = fields.String(required=False, allow_none=True, missing=None)
    profit_total = fields.Decimal(required=False, allow_none=True, missing=None)
    profit_total_usd = fields.Decimal(required=False, allow_none=True, missing=None)
    quote = fields.String(required=False, allow_none=True, missing=None)
    quote_fixed = fields.String(required=False, allow_none=True, missing=None)
    reactions = fields.String(required=False, allow_none=True, missing=None)
    receipt_times = fields.String(required=False, allow_none=True, missing=None)  # MARK
    receipt_url = fields.String(required=False, allow_none=True, missing=None)
    recipient = fields.String(required=False, allow_none=True, missing=None)
    reference = fields.String(required=False, allow_none=True, missing=None)
    remaining_dst = fields.Decimal(required=False, allow_none=True, missing=None)
    status = fields.String(required=False, allow_none=True, missing=None)
    stocks = fields.String(required=False, allow_none=True, missing=None)
    stocks_customer = fields.String(required=False, allow_none=True, missing=None)
    stocks_profit = fields.String(required=False, allow_none=True, missing=None)
    tags = fields.String(required=False, allow_none=True, missing=None)
    teller = fields.String(required=False, allow_none=True, missing=None)
    updated_at = AutoParsedDate(required=False, allow_none=True, missing=None)
    user_processor = fields.String(required=False, allow_none=True, missing=None)
    v = fields.String(required=False, allow_none=False, missing=None)
    v1 = fields.String(required=False, allow_none=False, missing=None)
    created_by_user_id = fields.String(required=False, allow_none=True, missing=None)
    created_by_user_first_name = fields.String(required=False, allow_none=True, missing=None)
    created_by_user_created_at = AutoParsedDate(required=False, allow_none=True, missing=None)
    customer_id = fields.String(required=False, allow_none=True, missing=None)
    customer_first_name = fields.String(required=False, allow_none=True, missing=None)
    customer_created_at = AutoParsedDate(required=False, allow_none=True, missing=None)
    teller_id = fields.String(required=False, allow_none=True, missing=None)
    teller_first_name = fields.String(required=False, allow_none=True, missing=None)
    teller_created_at = AutoParsedDate(required=False, allow_none=True, missing=None)
    user_processor_id = fields.String(required=False, allow_none=True, missing=None)
    user_processor_first_name = fields.String(required=False, allow_none=True, missing=None)
    user_processor_created_at = AutoParsedDate(required=False, allow_none=True, missing=None)
