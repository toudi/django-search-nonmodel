from search.database import DatabaseBackend, SearchResult
from search.document import Fields, Document
import os.path
from os import makedirs
from settings.paths import root
from whoosh import index, sorting
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC
from whoosh.qparser import MultifieldParser

class Backend(DatabaseBackend):
    index         = None
    writer        = None
    path          = None
    schema        = None
    search_fields = []
    stored_fields = []
    
    def __init__(self, name, schema, index_dir = ""):
        self.path   = index_dir + "/" + name
        self.schema = self.convert_schema(schema)
    
    def open(self, write = False):
        if not self.index:
            self.check_createdb()
            self.index = index.open_dir(self.path)
            
        if write and not self.writer:
            self.writer = self.index.writer()
    
    def check_createdb(self):
        if not os.path.exists(self.path):
            makedirs(self.path)
            
        if not index.exists_in(self.path):
            index.create_in(self.path, schema = self.schema)
            
    def commit(self):
        self.writer.commit()
        
    def close(self):
        self.index.close()
    
    def add(self, document):
        self.open(True)
        self.writer.add_document(**document.data)
        
    def replace(self, uuid, document):
        self.open(True)
        self.writer.update_document(**document.data)
        
    def remove(self, uuid):
        self.open(True)
        self.writer.delete_document(uuid)
    
    """
    Metoda konwertuje schemat bazy z ogolnego do szczegolowego (w tym wypadku whoosha)
    """
    def convert_schema(self, schema):
        parsed_schema = {}
        
        for field, field_type in schema.items():
            whoosh_field = TEXT(stored = field_type.store)
            if type(field_type) == Fields.UUIDField:
                whoosh_field = ID(stored = field_type.store, unique = True)
            elif type(field_type) == Fields.IntegerField:
                whoosh_field = NUMERIC(stored = field_type.store)
            if type(field_type) != Fields.UUIDField:
                self.search_fields.append(field)
            if field_type.store:
                self.stored_fields.append(field)
                
            parsed_schema[field] = whoosh_field
        
        return Schema(**parsed_schema)
        
    def find(self, query, order = None):
        self.open()
        return WhooshSearchResult(self, query, order)
        
    def rollback(self):
        self.writer.cancel()
        
class WhooshSearchResult(SearchResult):
    def __init__(self, engine, query, order):
        self.query  = query
        self.engine = engine
        self.order = None
        if order:
            self.order = sorting.MultiFacet()
            for field, sort_order in order.items():
                self.order.add_field(field, reverse = (sort_order == "asc"))
                
    def page(self, page, limit):
        with self.engine.index.searcher() as searcher:
            parser = MultifieldParser(
                self.engine.search_fields,
                schema = self.engine.index.schema
            )
            
            whoosh_query = parser.parse(self.query)
            
            results = searcher.search_page(whoosh_query, page, limit, sortedby = self.order)
            self.rows = results.total
            
            _results = []
            for result in results:
                _results.append(Document(data = {field: result.get(field, None) for field in self.engine.stored_fields}))
                
        return _results
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        pass