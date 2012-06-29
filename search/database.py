from django.db import settings
from search.query import Condition
from exceptions import InvalidFieldException

class Database(object):
    prefix = None
    engine = None
    schema = {}

    def __init__(self, dbname, engine = "default"):
        if not self.prefix:
            raise InvalidDatabasePrefixException("You must set the database prefix!")

        search = settings.SEARCH[engine]

        backend = search["backend"].split(".")

        imp = __import__(".".join(backend[:-1]), globals(), locals(), [backend[-1]])
        backend = getattr(imp,backend[-1])

        params = {
            "name"   : self.prefix + "/" + dbname,
            "schema" : self.schema
        }

        params.update(search.get("params", {}))

        self.engine = backend(**params)
        self.engine.database = self

    def add(self, document):
        self.validate_document(document)
        return self.engine.add(document)

    def replace_document(self, uuid, document):
        self.validate_document(document)
        return self.engine.replace(uuid, document)

    def remove_document(self, uuid):
        return self.engine.remove(uuid)

    def close(self):
        return self.engine.close()

    def begin(self):
        pass

    def rollback(self):
        return self.engine.rollback()

    def commit(self):
        return self.engine.commit()

    def find(self, query, order = None):
        return self.engine.find(query, order)

    def get(self, query):
        result = self.engine.find(query, None)
        page = result.page(1, 20)
        if len(page) > 0:
            return page[0].data
        return None

    def validate_document(self, document):
        if not document.is_uuid_present(self.schema):
            raise UUIDFieldNotPresentException()

        for field, value in document.data.items():
            #__isort to magiczne pole, ktore jest generowane dynamicznie,
            #bez wiedzy uzytkownika.
            if not self.schema.has_key(field) and "__isort" not in field:
                raise InvalidFieldException("%s not in %s" % (field, ",".join(self.schema.keys())))


class DatabaseBackend(object):
    database = None
    def __init__(self, name):
        raise NeedToReimplementThisMethodException("__init__")

    def add(self, document):
        raise NeedToReimplementThisMethodException("add(document)")

    def rollback(self):
        pass

    def replace(self, document):
        raise NeedToReimplementThisMethodException("replace(document)")

    def parseQueryCondition(self, condition):
        if condition.operator == Condition.OPERATOR_CONTAINS:
            return u'%s:%s' % (condition.field, condition.value)
        else:
            return u'%s:%s%s' % (condition.field, condition.operator, condition.value)


class SearchResult:
    rows = 0

    def page(self, page, limit):
        pass

    """
    Ta metoda zwalnia uchwyt do searchera
    """
    def free(self):
        pass
