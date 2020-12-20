import apache_beam as beam


class UnnestGroupedCollections(beam.DoFn):
    def __init__(self, left_key: str, join_key: str):
        super().__init__()
        self.left_key = left_key
        self.join_key = join_key

    def process(self, input):
        key, value = input
        left_coll = value[self.left_key]
        join_coll = value[self.join_key]

        for value in left_coll:
            try:
                yield value.update(join_coll[0])
            except IndexError:
                yield value


class LeftJoin(beam.PTransform):
    def __init__(self, left_key: str, join_key: str):
        super().__init__()
        self.left_key = left_key
        self.join_key = join_key

    def expand(self, p):
        left_coll, join_coll = p
        left_map = (left_coll | 'LeftMapKey' >> beam.Map(lambda value: (str(value[self.left_key]), value)))
        join_map = (join_coll | 'RightMapKey' >> beam.Map(lambda value: (str(value[self.join_key]), value)))

        return ({'left': left_map, 'join': join_map}
                | 'GroupByJoinKey' >> beam.CoGroupByKey()
                | 'UnnestGroupedCollections' >> beam.ParDo(UnnestGroupedCollections('left', 'join')))
