import datetime

class Document(object):
    raw_data = {}
    data = {}
    _schema     = None
    _uuid_field = None
    _restore = False

    def __init__(self, data = {}, restore = False):
        self._restore = restore
        self.set_data(data)

    def set_data(self, data):
        _data = {}
        self.raw_data = data
        for field, value in data.items():
            _value = value
            if type(value) == str:
                _value = unicode(value, "utf-8")
            _data[field] = _value
        self.data = _data
        if not self._restore:
            self.process()
        else:
            self.restore()

    def process(self):
        pass

    def restore(self):
        pass

    def is_uuid_present(self, schema):
        if not self._schema:
            self._schema = schema
        return self.uuid_field is not None and self.data[self.uuid_field] is not None

    @property
    def uuid_field(self):
        if not self._uuid_field:
            for field, field_type in self._schema.items():
                if type(field_type) == Fields.UUIDField:
                    self._uuid_field = field
                    break

        return self._uuid_field

    def get_uuid(self):
        if self.is_uuid_present:
            return self.data[self.uuid_field]
        return None

    def serialize(self, columns):
        out = {}
        for column in columns:
            value = self.data.get(column)
            if value is not None:
                if type(value) == datetime.datetime:
                    value = str(value)
                out[column] = value
        return out

class Fields(object):
    class Field(object):
        store = False

        def __init__(self, store = False, analyze = False, sort = False, split_to_terms = True):
            self.store = store
            self.analyze = analyze
            self.sort = sort
            self.split_to_terms = split_to_terms

    class CharField(Field):
        pass

    class UUIDField(Field):
        pass

    class IntegerField(Field):
        pass
