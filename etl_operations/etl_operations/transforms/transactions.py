
import datetime
import decimal
import apache_beam as beam


def filter_transaction(transaction):
    return transaction['id'] and transaction['user_id']


def filter_currency_operation(transaction, src_currency: str, dst_currency: str):
    source_asset = str(transaction['asset_src']).lower() if transaction['asset_src'] else None
    destination_asset = str(transaction['asset_dst']).lower() if transaction['asset_dst'] else None
    return source_asset == src_currency.lower() and destination_asset == dst_currency.lower()


class TransformTransaction(beam.DoFn):
    def process(self, transaction):
        def __parse_date__(date):
            if date and isinstance(date, datetime.datetime):
                return date.strftime('%Y-%m-%d %H:%M:%S')
            elif date and isinstance(date, str):
                return date
            else:
                return None

        def __parse_decimal__(number):
            if number and isinstance(number, decimal.Decimal):
                return str(number)
            else:
                return number
        yield {
            'id': transaction['id'],
            'id2': transaction['id2'],
            'account_id_dst': transaction['account_id_dst'],
            'account_id_src': transaction['account_id_src'],
            'amount_dst': __parse_decimal__(transaction['amount_dst']),
            'amount_dst_usd': __parse_decimal__(transaction['amount_dst_usd']),
            'amount_src': __parse_decimal__(transaction['amount_src']),
            'amount_src_usd': __parse_decimal__(transaction['amount_src_usd']),
            'asset_dst': transaction['asset_dst'],
            'asset_src': transaction['asset_src'],
            'contact_dst': transaction['contact_dst'],
            'created_at': __parse_date__(transaction['created_at']),
            'description': transaction['description'],
            'order_id': transaction['order_id'],
            'service_name': transaction['service_name'],
            'short_id': transaction['short_id'],
            'state': transaction['state'],
            'sync_date': __parse_date__(transaction['sync_date']),
            'type': transaction['type'],
            'updated_at': __parse_date__(transaction['updated_at']),
            'user_id': transaction['user_id'],
            'user_type': transaction['user_type'],
            'v': transaction['v'],
            'wallet_dst': transaction['wallet_dst'],
            'wallet_src': transaction['wallet_src'],
        }
