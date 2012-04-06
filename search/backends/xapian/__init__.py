from search.database import DatabaseBackend, SearchResult
from search.document import Fields, Document
import os.path
from os import makedirs
from settings.paths import root
import xappy
import re

class Backend(DatabaseBackend):
    connection    = None
    path          = None
    schema        = None
    
    def __init__(self, name, schema, index_dir = ""):
        self.path   = index_dir + "/" + name
        self.schema = schema
    
    def open(self, write = False):
        if not self.connection:
            if write:
                self.check_createdb()
                self.connection = xappy.IndexerConnection(self.path)
                self.prepare_schema()
            else:
                self.connection = xappy.SearchConnection(self.path)
    
    def check_createdb(self):
        if not os.path.exists(self.path):
            makedirs(self.path)
            
    def commit(self):
        self.connection.flush()
        
    def close(self):
        if self.connection:
            self.connection.close()
    
    def add(self, document):
        self.open(True)
        doc = self._prepare_document(document)
        self.connection.add(doc)
        
    def replace(self, uuid, document):
        self.open(True)
        self.connection.replace(uuid, self._prepare_document(document))
        
    def remove(self, uuid):
        self.open(True)
        self.connection.delete(uuid)
        
    
    def prepare_schema(self):
        for field, field_type in self.schema.items():
            sort_type = None
            if type(field_type) == Fields.IntegerField:
                sort_type = "float"
            elif type(field_type) == Fields.UUIDField:
                self.connection.add_field_action(field, xappy.FieldActions.INDEX_EXACT)
            elif type(field_type) == Fields.CharField:
                self.connection.add_field_action(field, xappy.FieldActions.INDEX_FREETEXT)
            if field_type.store:
                self.connection.add_field_action(field, xappy.FieldActions.STORE_CONTENT)
            if field_type.sort:
                self.connection.add_field_action(field, xappy.FieldActions.SORTABLE, type = sort_type)
        
    def find(self, query, order = None):
        self.open()
        return XapianSearchResult(self, query, order)
        
    def _prepare_document(self, document):
        doc = xappy.UnprocessedDocument()
        for field, content in document.data.items():
            doc.fields.append(xappy.Field(field, content))
        if document.is_uuid_present:
            doc.id = document.data[document.uuid_field]
        return doc
                
class XapianSearchResult(SearchResult):
    def __init__(self, engine, query, order):
        for field in engine.schema:
            query = re.sub(field+":\"(.*?)\"", field+":\\1", query)
            
        self.query  = engine.connection.query_parse(query, allow_wildcards = True)
        self.engine = engine
        self.order = None
        if order:
            self.order = []
            for field, sort_order in order.items():
                order_str = field
                if sort_order == "desc":
                    order_str = "-" + order_str
                self.order.append(order_str)
            self.order = ",".join(self.order)

    def page(self, page, limit):
        _results = []
        
        search_result = self.engine.connection.search(self.query, ((page-1) * limit), limit, sortby = self.order)
        
        self.rows = search_result.matches_estimated
        
        for result in search_result:
            data = {}
            for field, values in result.data.items():
                if type(self.engine.schema[field]) == Fields.IntegerField:
                    data[field] = int(values[0])
                else:
                    data[field] = " ".join(values)
                    
            _results.append(Document(data))
            
        return _results
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args, **kwargs):
        pass