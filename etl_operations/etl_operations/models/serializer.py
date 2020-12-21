
import datetime
from marshmallow import fields


class AutoParsedDate(fields.DateTime):

    def __parse_from_date_object__(self, date):
        if isinstance(date, datetime.date):
            return datetime.datetime.combine(date, datetime.datetime.min.time()).isoformat()
        elif isinstance(date, datetime.datetime):
            return date.isoformat()
        else:
            return date

    def _deserialize(self, value, attr, data, **kwargs):
        new_value = self.__parse_from_date_object__(value)
        return super()._deserialize(new_value, attr, data, **kwargs)
