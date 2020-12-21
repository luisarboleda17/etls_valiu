
import logging
import apache_beam as beam
from etl_operations.models.core import TransactionSchema


def filter_transaction(transaction):
    schema = TransactionSchema()
    errors = schema.validate(transaction, partial=("transaction_type",))
    logging.info(f'{errors} - {errors == {}} - {transaction}')
    return errors == {}


def filter_currency_operation(transaction, src_currency: str, dst_currency: str):
    source_asset = str(transaction['asset_src']).lower() if transaction['asset_src'] else None
    destination_asset = str(transaction['asset_dst']).lower() if transaction['asset_dst'] else None
    return source_asset == src_currency.lower() and destination_asset == dst_currency.lower()


class TransformTransaction(beam.DoFn):
    def __init__(self, transaction_type: str):
        super().__init__()
        self.transaction_type = transaction_type

    def process(self, transaction):
        schema = TransactionSchema()
        transaction.update({'transaction_type': self.transaction_type})
        yield schema.dump(transaction)
