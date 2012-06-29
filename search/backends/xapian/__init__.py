from search.database import DatabaseBackend, SearchResult
from search.document import Fields
from search.query import Condition
from search.exceptions import DatabaseLockedException
import os.path
from os import makedirs
import xappy


class Backend(DatabaseBackend):
    connection    = None
    path          = None
    schema        = None
    ranges        = {}
    mappings      = {}
    values        = []
    sort_fields   = []

    def __init__(self, name, schema, index_dir = ""):
        self.path   = index_dir + "/" + name
        self.schema = schema

    def open(self, write = False):
        if not self.connection:
            if write:
                try:
                    self.check_createdb()
                    self.connection = xappy.IndexerConnection(self.path)
                    self.prepare_schema()
                except xappy.XapianDatabaseLockError:
                    raise DatabaseLockedException
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
            self.connection = None

    def add(self, document):
        self.open(True)
        doc = self._prepare_document(document)
        doc = self.connection.process(doc)
        self.connection.add(doc)

    def replace(self, uuid, document):
        self.open(True)
        self.connection.replace(self._prepare_document(document))

    def remove(self, uuid):
        self.open(True)
        self.connection.delete(uuid)


    def prepare_schema(self):
        for field, field_type in self.schema.items():
            sort_type = None
            if type(field_type) == Fields.IntegerField:
                sort_type = "float"
                field_type.sort = True
                self.connection.add_field_action(field, xappy.FieldActions.INDEX_EXACT)
                self.mappings[field] = str
                self.values.append(field)
            elif type(field_type) == Fields.UUIDField:
                self.connection.add_field_action(field, xappy.FieldActions.INDEX_EXACT)
            elif type(field_type) == Fields.CharField:
                self.connection.add_field_action(field, xappy.FieldActions.INDEX_FREETEXT)
                if field_type.sort:
                    self.sort_fields.append(field)
                    self.connection.add_field_action(field+"__isort", xappy.FieldActions.INDEX_FREETEXT)
                    self.connection.add_field_action(field+"__isort", xappy.FieldActions.SORTABLE, type="string")
            if field_type.store:
                self.connection.add_field_action(field, xappy.FieldActions.STORE_CONTENT)
            if field_type.sort:
                self.connection.add_field_action(field, xappy.FieldActions.SORTABLE, type = sort_type)

    def get_sort_fields(self):
        if not self.sort_fields:
            for field, field_type in self.schema.items():
                if type(field_type) == Fields.CharField and field_type.sort:
                    self.sort_fields.append(field)
        return self.sort_fields

    def find(self, query, order = None):
        self.open()
        return XapianSearchResult(self, query, order)

    def _prepare_document(self, document):
        doc = xappy.UnprocessedDocument()
        for field, content in document.data.items():
            if field in self.mappings:
                content = self.mappings[field](content)
            if field in self.sort_fields:
                try:
                    sort_content = content.replace(" ", "").lower()
                except:
                    sort_content = ""
                doc.fields.append(xappy.Field(field+"__isort", sort_content))
            doc.fields.append(xappy.Field(field, content))

        if document.is_uuid_present:
            doc.id = document.data[document.uuid_field]

        return doc

    def parseQueryCondition(self, condition):
        if condition.operator in [
            Condition.OPERATOR_LESS_THAN, Condition.OPERATOR_LESS_EQUAL
        ]:
            if condition.field not in self.ranges:
                self.ranges[condition.field] = {}
            self.ranges[condition.field]["end"] = condition.value

            return None
            return "%s:..%s" % (condition.field, condition.value)
        elif condition.operator in [
            Condition.OPERATOR_GREATER_THAN, Condition.OPERATOR_GREATER_EQUAL
        ]:
            if condition.field not in self.ranges:
                self.ranges[condition.field] = {}
            self.ranges[condition.field]["start"] = condition.value

            return None
            return "%s:%s.." % (condition.field, condition.value)
        else:
            return super(Backend, self).parseQueryCondition(condition)

class XapianSearchResult(SearchResult):
    def __init__(self, engine, query, order, order_case_insensitive=True):
        engine.ranges = {}
        query = query.toString(engine)

        self.query = engine.connection.query_parse(query, allow_wildcards=True)

        if engine.ranges:
            if not len(query.strip()):
                self.query = engine.connection.query_all()
            for field, _range in engine.ranges.items():
                self.query = engine.connection.query_filter(
                    self.query,
                    engine.connection.query_range(
                        field,
                        _range.get("start"),
                        _range.get("end")
                    )
                )
        self.engine = engine
        self.order = None
        if order:
            self.order = []
            for field, sort_order in order.items():
                order_str = field
                if order_case_insensitive and field in self.engine.get_sort_fields():
                    order_str += "__isort"
                if sort_order == "desc":
                    order_str = "-" + order_str
                self.order.append(order_str)

    def page(self, page, limit):
        _results = []

        range_start = (page - 1) * limit
        range_end   = range_start + limit

        search_result = self.engine.connection.search(
            self.query,
            range_start,
            range_end,
            sortby=self.order
        )

        self.rows = search_result.matches_estimated

        doc_class = self.engine.database.document

        for result in search_result:
            data = {}
            for field, values in result.data.items():
                if type(self.engine.schema[field]) == Fields.IntegerField:
                    data[field] = int(values[0])
                else:
                    data[field] = " ".join(values)

            _results.append(doc_class(data, restore=True))

        return _results

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass
