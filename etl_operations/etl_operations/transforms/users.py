
import apache_beam as beam
from etl_operations.models.auth import UserSchema


def filter_user(user):
    schema = UserSchema()
    errors = schema.validate(user)
    return errors == {}


class TransformUser(beam.DoFn):
    def process(self, user):
        schema = UserSchema()
        new_user = {
            'id': user['_id'],
            'first_name': user['firstName'],
            'created_at': user['createdAt']
        }
        yield schema.dump(new_user)
