class Document(object):
    data = {}
    _schema     = None
    _uuid_field = None
    
    def __init__(self, data = {}):
        self.set_data(data)
        
    def set_data(self, data):
        self.data = data
        self.process()
    
    def process(self):
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
    
class Fields(object):
    class Field(object):
        store = False
        
        def __init__(self, store = False, analyze = False, sort = False):
            self.store = store
            self.analyze = analyze
            self.sort = sort
        
    class CharField(Field):
        pass
    
    class UUIDField(Field):
        pass
    
    class IntegerField(Field):
        pass