import sys
import argparse
import logging
import apache_beam as beam
from apache_beam.io.mongodbio import ReadFromMongoDB
from beam_nuggets.io.relational_db import ReadFromDB, SourceConfiguration
from apache_beam.options.pipeline_options import PipelineOptions
from etl_operations.transforms.left_join import LeftJoin


def __filter_transaction__(transaction):
    return transaction['id'] and transaction['user_id']


def __filter_user__(user):
    return user['_id']


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

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
                     | 'FilterTransactions' >> beam.Filter(__filter_transaction__))

        read_auth = (p
                     | 'Read Users' >> ReadFromMongoDB(
                         uri='',
                         db='auth',
                         coll='users'
                     )
                     | 'FilterUsers' >> beam.Filter(__filter_user__))

        ((read_core, read_auth)
         | LeftJoin('id2', '_id')
         | 'Show results' >> beam.ParDo(print))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
