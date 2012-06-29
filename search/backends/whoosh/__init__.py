from search.database import DatabaseBackend, SearchResult
from search.document import Fields, Document
from search.exceptions import DatabaseLockedException
from search.query import Condition

import os.path
from os import makedirs
from settings.paths import root
from whoosh import index, sorting
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC
from whoosh.qparser import MultifieldParser, GtLtPlugin, WildcardPlugin, PrefixPlugin, PhrasePlugin, FieldsPlugin
from whoosh.filedb.filewriting import LockError

import sys

class Backend(DatabaseBackend):
    index         = None
    writer        = None
    path          = None
    schema        = None
    search_fields = []
    stored_fields = []
    uuid_field    = None
    sort_fields   = []

    def __init__(self, name, schema, index_dir = ""):
        self.path   = index_dir + "/" + name
        self.schema = self.convert_schema(schema)

    def open(self, write = False):
        if not self.index:
            self.check_createdb()
            self.index = index.open_dir(self.path)

        if write and not self.writer:
            try:
                self.writer = self.index.writer()
            except LockError:
                raise DatabaseLockedException

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
        document = self._prepare_document(document)
        self.writer.add_document(**document.data)

    def replace(self, uuid, document):
        self.open(True)
        document = self._prepare_document(document)
        self.writer.update_document(**document.data)

    def remove(self, uuid):
        self.open(True)
        self.writer.delete_by_term(self.uuid_field, uuid)

    """
    Metoda konwertuje schemat bazy z ogolnego do szczegolowego (w tym wypadku whoosha)
    """
    def convert_schema(self, schema):
        parsed_schema = {}

        for field, field_type in schema.items():
            whoosh_field = TEXT(stored = field_type.store)
            if type(field_type) == Fields.UUIDField:
                whoosh_field = ID(stored = field_type.store, unique = True)
                self.uuid_field = field
            elif type(field_type) == Fields.IntegerField:
                whoosh_field = NUMERIC(stored = field_type.store)
            elif type(field_type) == Fields.CharField and field_type.split_to_terms == False:
                whoosh_field = ID(stored = field_type.store)
            if type(field_type) != Fields.UUIDField:
                self.search_fields.append(field)
            if field_type.store:
                self.stored_fields.append(field)
            if type(field_type) == Fields.CharField and field_type.sort:
                parsed_schema[field+"__isort"] = ID(stored=False)
                self.sort_fields.append(field)
            parsed_schema[field] = whoosh_field

        return Schema(**parsed_schema)

    def find(self, query, order = None):
        self.open()
        return WhooshSearchResult(self, query, order)

    def rollback(self):
        self.writer.cancel()

    def _prepare_document(self, document):
        if self.sort_fields:
            for field in self.sort_fields:
                sort_content = ""
                try:
                    sort_content = document.data[field].replace(" ", "").lower()
                except:
                    pass
                if type(sort_content) == str:
                    sort_content = unicode(sort_content, "utf-8")
                document.data[field+"__isort"] = sort_content
        for field, content in document.data.items():
            if type(content) == str or type(content) == unicode and len(content) == 0:
                document.data[field] = u" "

        return document


class WhooshSearchResult(SearchResult):
    def __init__(self, engine, query, order):
        self.query  = query
        self.engine = engine
        self.order = None
        if order:
            self.order = sorting.MultiFacet()
            for field, sort_order in order.items():
                order_field = field
                if order_field in engine.sort_fields:
                    order_field = field + "__isort"
                self.order.add_field(order_field, reverse = (sort_order == "desc"))

    def page(self, page, limit):
        with self.engine.index.searcher() as searcher:
            parser = MultifieldParser(
                self.engine.search_fields,
                schema = self.engine.index.schema,
            )
            parser.add_plugin(GtLtPlugin())
            parser.add_plugin(PhrasePlugin())
            parser.add_plugin(FieldsPlugin())
            #parser.remove_plugin_class(WildcardPlugin)
            #parser.add_plugin(WildcardPlugin())
            parser.add_plugin(PrefixPlugin())

            whoosh_query = parser.parse(self.query.toString(self.engine))
            #print "============" + str(whoosh_query)
            results = searcher.search_page(whoosh_query, page, limit, sortedby = self.order)
            self.rows = results.total

            _results = []

            doc_class = self.engine.database.document

            for result in results:
                doc = doc_class(data = {field: result.get(field, None) for field in self.engine.stored_fields}, restore = True)
                _results.append(doc)

        return _results

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
