
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.mongodbio import ReadFromMongoDB
from beam_nuggets.io.relational_db import ReadFromDB, SourceConfiguration
from etl_operations.transforms.left_join import LeftJoin
from etl_operations.transforms.transactions import filter_currency_operation, filter_transaction, TransformTransaction
from etl_operations.transforms.users import filter_user


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
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
            {'name': 'created_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'service_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'short_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sync_date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'updated_at', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'user_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'v', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'wallet_dst', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'wallet_src', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        read_core = (p
                     | 'ReadCore' >> ReadFromDB(
                         source_config=SourceConfiguration(
                             drivername='postgresql+psycopg2',
                             host='',
                             port=0,
                             username='',
                             password='',
                             database='',
                             create_if_missing=False
                         ),
                         table_name='transaction',
                         query='SELECT * FROM transaction LIMIT 10'
                     )
                     | 'FilterTransactions' >> beam.Filter(filter_transaction))

        read_auth = (p
                     | 'Read Users' >> ReadFromMongoDB(
                         uri='',
                         db='auth',
                         coll='users'
                     )
                     | 'FilterUsers' >> beam.Filter(filter_user))

        ((read_core, read_auth)
                                      | LeftJoin('id2', '_id')
                                      | 'FilterCashIn' >> beam.Filter(filter_currency_operation, 'USDv', 'USDv')
                                      | 'TransformDate' >> beam.ParDo(TransformTransaction())
                                      | 'WriteCashIn' >> beam.io.WriteToBigQuery(
                                          table='core_transactions',
                                          dataset='core',
                                          project='',
                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                          schema=table_schema
                                      ))


        # write_transactions_cash_out = (core_transactions
        #                                | 'FilterCashOut' >> beam.Filter(__filter_currency_operation__, 'USDv', 'VES'))
        #
        # write_transactions_p2p = (core_transactions
        #                           | 'FilterP2P' >> beam.Filter(__filter_currency_operation__, 'USDv', 'USDv'))
        #
        # write_core_transactions = (core_transactions
        #                            | beam.io.WriteToBigQuery(table='core_transactions', ))

