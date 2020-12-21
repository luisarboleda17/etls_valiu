
import logging
import apache_beam as beam
from etl_operations.models.remittances import RemittanceSchema
from etl_operations.transforms.left_join import LeftJoin


def filter_remittance(remittance):
    schema = RemittanceSchema()
    errors = schema.validate(remittance)
    logging.info(f'{errors} - {errors == {}} - {remittance}')
    return errors == {}


class TransformRemittance(beam.DoFn):
    def process(self, remittance):
        schema = RemittanceSchema()
        parsed_id = str(remittance['id']) if remittance['id'] else None
        parsed_created = str(remittance['created_via_app_version']) if remittance['created_via_app_version'] else None
        parsed_receipt_times = str(remittance['id']) if remittance['id'] else None
        remittance.update({
            'id': parsed_id,
            'created_via_app_version': parsed_created,
            'receipt_times': parsed_receipt_times,
        })
        yield schema.dump(remittance)


class MergeRemittancesUsers(beam.PTransform):
    def expand(self, p):
        read_remittances, read_auth = p
        merged_remittances_created_user = ((read_remittances, read_auth)
                                           | 'MergeRemittancesCreatedUsers' >> LeftJoin(
                                               'created_by_user',
                                               'id',
                                               'created_by_user_'
                                           ))
        merged_remittances_customers = ((merged_remittances_created_user, read_auth)
                                        | 'MergeRemittancesCustomers' >> LeftJoin('customer', 'id', 'customer_'))
        merged_remittances_tellers = ((merged_remittances_customers, read_auth)
                                      | 'MergeRemittancesTellers' >> LeftJoin('teller', 'id', 'teller_'))
        return ((merged_remittances_tellers, read_auth)
                | 'MergeRemittancesUserProcessors' >> LeftJoin(
                    'user_processor',
                    'id',
                    'user_processor_'
                ))
