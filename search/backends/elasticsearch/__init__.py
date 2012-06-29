from search.database import DatabaseBackend, SearchResult
from search.document import Document, Fields
import pyes
from pyes.mappings import StringField, IntegerField
from pyes.query import Search, StringQuery
from pyes.exceptions import IndexMissingException

class Backend(DatabaseBackend):
    name       = None
    schema     = None
    hosts      = None
    connection = None
    stored_fields = []
    
    def __init__(self, name, schema, hosts):
        name = name.split("/")
        self.name   = "_".join(name[1:])
        self.type   = name[0]
        self.schema = schema
        self.hosts  = hosts
        self.schema = self.convert_schema(schema)
        
    def open(self, write = False):
        if not self.connection:
            self.connection = pyes.ES(self.hosts)
        if write:
            try:
                self.connection.open_index(self.name)
            except IndexMissingException:
                self.connection.create_index(self.name)
                self.connection.put_mapping(self.type, self.schema)
    
    def add(self, document):
        self.open(True)
        self.connection.index(document.data, self.name, self.type, id = document.get_uuid(), bulk = True)
        
    def replace(self, uuid, document):
        return self.add(document)
    
    def remove(self, uuid):
        self.open(True)
        self.connection.delete(self.name, self.type, uuid)
        
    def commit(self):
        self.connection.refresh([self.name])
        
    def close(self):
        pass
        
    def convert_schema(self, schema):
        out = {}
        for field, field_type in schema.items():
            esfield = StringField
            if type(field_type) == Fields.IntegerField:
                esfield = IntegerField
            esfield = esfield(store = "yes" if field_type.store else "no")
            if field_type.analyze:
                esfield.index = "analyzed"
            esfield = esfield.as_dict()
            out[field] = esfield
            if field_type.store:
                self.stored_fields.append(field)
        return out
    
    def find(self, query, order = {}):
        self.open()
        return ElasticSearchResult(self, query, order)
    
class ElasticSearchResult(SearchResult):
    def __init__(self, engine, query, order):
        self.query  = query
        self.order = order
        self.engine = engine

    def page(self, page, limit):
        _results = []
        search_result = self.engine.connection.search(
            Search(StringQuery(self.query),
            sort = self.order,
            size = limit,
            start = ((page - 1) * limit)),
            indexes = [self.engine.name]
        )
        
        self.rows = search_result.total
        doc_class = self.engine.database.document
        
        for result in search_result["hits"]["hits"]:
            _results.append(
                Document(
                    data = {field: result["_source"].get(field, None) for field in self.engine.stored_fields},
                    restore = True
                )
            )
        return _results
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        pass    