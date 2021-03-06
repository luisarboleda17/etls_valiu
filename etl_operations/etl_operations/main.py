
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.mongodbio import ReadFromMongoDB
from beam_nuggets.io.relational_db import ReadFromDB, SourceConfiguration
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL
from etl_operations.transforms.left_join import LeftJoin
from etl_operations.transforms.transactions import filter_currency_operation, filter_transaction, TransformTransaction
from etl_operations.transforms.users import filter_user, TransformUser
from etl_operations.transforms.remittances import TransformRemittance, filter_remittance, MergeRemittancesUsers
from etl_operations.models.core import transactions_table_partitioning, transactions_table_schema
from etl_operations.models.auth import users_table_schema, users_table_partitioning
from etl_operations.models.remittances import remittances_table_partitioning, remittances_table_schema


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--core_host', dest='core_host', type=str)
    parser.add_argument('--core_port', dest='core_port', type=int)
    parser.add_argument('--core_username', dest='core_username', type=str)
    parser.add_argument('--core_password', dest='core_password', type=str)
    parser.add_argument('--core_database', dest='core_database', type=str)
    parser.add_argument('--remittances_host', dest='remittances_host', type=str)
    parser.add_argument('--remittances_port', dest='remittances_port', type=int)
    parser.add_argument('--remittances_username', dest='remittances_username', type=str)
    parser.add_argument('--remittances_password', dest='remittances_password', type=str)
    parser.add_argument('--remittances_database', dest='remittances_database', type=str)
    parser.add_argument('--auth_uri', dest='auth_uri', type=str)
    parser.add_argument('--auth_database', dest='auth_database', type=str)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    project_id = pipeline_options.display_data()['project']

    with beam.Pipeline(options=pipeline_options) as p:
        # Reads from data sources

        read_core = (p
                     | 'ReadCore' >> ReadFromDB(
                         source_config=SourceConfiguration(
                             drivername='postgresql+psycopg2',
                             host=known_args.core_host,
                             port=known_args.core_port,
                             username=known_args.core_username,
                             password=known_args.core_password,
                             database=known_args.core_database,
                             create_if_missing=False
                         ),
                         table_name='transaction',
                         query='SELECT * FROM transaction'
                     )
                     | 'FilterTransactions' >> beam.Filter(filter_transaction))

        read_auth = (p
                     | 'Read Users' >> ReadFromMongoDB(
                         uri=known_args.auth_uri,
                         db=known_args.auth_database,
                         coll='users'
                     )
                     | 'TransformUsers' >> beam.ParDo(TransformUser())
                     | 'FilterUsers' >> beam.Filter(filter_user))

        read_remittances = (p
                            | 'ReadRemittances' >> ReadFromMySQL(
                                query='SELECT * FROM valiu_remittances.remittance',
                                host=known_args.remittances_host,
                                database=known_args.remittances_database,
                                user=known_args.remittances_username,
                                password=known_args.remittances_password,
                                port=known_args.remittances_port,
                                splitter=splitters.NoSplitter()
                            )
                            | 'TransformRemittances' >> beam.ParDo(TransformRemittance())
                            | 'FilterRemittances' >> beam.Filter(filter_remittance))

        # Merges

        merged_transactions = ((read_core, read_auth) | 'MergeTransactionsUsers' >> LeftJoin('id2', 'id', 'user_'))

        merged_remittances = ((read_remittances, read_auth)
                              | 'MergeRemittanceUsers' >> MergeRemittancesUsers())

        # Writes to BigQuery

        write_users = (read_auth
                       | 'WriteUsers' >> beam.io.WriteToBigQuery(
                           table='auth_users',
                           dataset='auth',
                           project=project_id,
                           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                           schema=users_table_schema,
                           additional_bq_parameters=users_table_partitioning
                       ))

        write_remittances = (merged_remittances
                             | 'WriteRemittances' >> beam.io.WriteToBigQuery(
                                 table='remittances_movements',
                                 dataset='remittances',
                                 project=project_id,
                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                 schema=remittances_table_schema,
                                 additional_bq_parameters=remittances_table_partitioning
                             ))

        write_transactions_cash_in = (merged_transactions
                                      | 'FilterCashIn' >> beam.Filter(filter_currency_operation, 'COP', 'USDv')
                                      | 'CleanTransactionsCashIn' >> beam.ParDo(TransformTransaction('cash_in'))
                                      | 'WriteCashIn' >> beam.io.WriteToBigQuery(
                                          table='core_cash_in',
                                          dataset='core',
                                          project=project_id,
                                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                          schema=transactions_table_schema,
                                          additional_bq_parameters=transactions_table_partitioning
                                      ))

        write_transactions_cash_out = (merged_transactions
                                       | 'FilterCashOut' >> beam.Filter(filter_currency_operation, 'USDv', 'VES')
                                       | 'CleanTransactionsCashOut' >> beam.ParDo(TransformTransaction('cash_out'))
                                       | 'WriteCashOut' >> beam.io.WriteToBigQuery(
                                           table='core_cash_out',
                                           dataset='core',
                                           project=project_id,
                                           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                           schema=transactions_table_schema,
                                           additional_bq_parameters=transactions_table_partitioning
                                       ))

        write_transactions_p2p = (merged_transactions
                                  | 'FilterP2P' >> beam.Filter(filter_currency_operation, 'USDv', 'USDv')
                                  | 'CleanTransactionsP2P' >> beam.ParDo(TransformTransaction('p2p'))
                                  | 'WriteP2P' >> beam.io.WriteToBigQuery(
                                      table='core_p2p',
                                      dataset='core',
                                      project=project_id,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                      schema=transactions_table_schema,
                                      additional_bq_parameters=transactions_table_partitioning
                                  ))
