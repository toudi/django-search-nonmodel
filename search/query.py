class Conjunction:
    AND = " AND "
    OR  = " OR "

class Condition:
    OPERATOR_CONTAINS     = ":"
    OPERATOR_LESS_THAN    = "<"
    OPERATOR_LESS_EQUAL   = "<="
    OPERATOR_GREATER_THAN = ">"
    OPERATOR_GREATER_EQUAL= ">="
    field    = ""
    value    = ""
    operator = ""

    def __init__(self, field, value, operator = OPERATOR_CONTAINS):
        self.field    = field
        self.value    = value
        self.operator = operator

    def __str__(self):
        return "%s %s %s" % (self.field, self.operator, self.value)

    def toString(self, engine):
        return engine.parseQueryCondition(self)

class Query:
    operators = {
        "__le" : Condition.OPERATOR_LESS_EQUAL,
        "__lt" : Condition.OPERATOR_LESS_THAN,
        "__ge" : Condition.OPERATOR_GREATER_EQUAL,
        "__gt" : Condition.OPERATOR_GREATER_THAN
    }
    conjunction = Conjunction.AND
    conditions = []
    subqueries = []

    def __init__(self, *args, **kwargs):
        self.subqueries = args
        self.conditions = []
        for field, value in kwargs.items():
            operator = Condition.OPERATOR_CONTAINS
            f = field
            for op in self.operators:
                if op in field:
                    field = field.split("__")
                    f = field[0]
                    operator = self.operators["__" + field[1]]
            self.conditions.append(Condition(f, value, operator))

    def __str__(self):
        _repr = self.conjunction.join(str(cond) for cond in self.conditions)
        if self.subqueries:
            _repr += self.conjunction + "(" + self.conjunction.join([str(sq) for sq in self.subqueries]) + ")"
        return _repr

    def toString(self, engine):
        conditions_str = []
        for cond in self.conditions:
            cond_str = cond.toString(engine)
            if cond_str:
                conditions_str.append(cond_str)

        _repr = self.conjunction.join(conditions_str)

        if self.subqueries:
            subq_str = self.conjunction.join(
                [sq.toString(engine) for sq in self.subqueries]
            )
            if len(subq_str.strip()):
                if len(_repr.strip()) > 0:
                    _repr += self.conjunction
                _repr += "(" + subq_str + ")"

        return _repr

class OR(Query):
    conjunction = Conjunction.OR
