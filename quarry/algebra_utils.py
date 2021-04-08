from typing import List, Set, Tuple, Any, Dict

from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.plugins.sparql.sparql import Query

from .classes import Operator, Expression, Term, Triple

literal_counter = 0
uri_counter = 0


def new_literal():
    global literal_counter
    literal = 'literal_' + str(literal_counter)
    literal_counter += 1
    return literal


def new_uri():
    global uri_counter
    uri = 'uri_' + str(uri_counter)
    uri_counter += 1
    return uri


def from_rdflib_sparqlquery(query: Query) -> Operator:
    root_operator = from_query(query)
    return root_operator


def from_query(q: Query) -> Operator:
    return from_comp_value_rec('algebra', q.algebra, {})


def from_comp_value_rec(name: str, comp_value: CompValue, term_dict: Dict) -> Operator:
    children_comp_values = find_children(comp_value)
    children_operators = set(from_comp_value_rec(name, c, term_dict) for (name, c) in children_comp_values)
    operator = from_comp_value(name, comp_value, children_operators, term_dict)
    return operator


def from_comp_value(name, cv: CompValue, children_operators: Set[Operator], term_dict: Dict) -> Operator:
    if 'expr' in cv and type(cv['expr']) == Expr and cv['expr'].name != 'TrueFilter':
        expressions = from_expression(cv['expr'], term_dict)
    else:
        expressions = set()

    if cv.name in {'SelectQuery', 'Project', 'LeftJoin'}:
        triples = set()
        operator = Operator(name=name, type=cv.name, children=children_operators,
                            triples=triples, expressions=expressions)
        if cv.name == 'SelectQuery':
            operator.project_vars = [from_rdflib_term(v, term_dict) for v in cv['PV']]
        return operator
    elif cv.name in {'BGP'}:
        triples = set(Triple(subject=from_rdflib_term(t[0], term_dict), verb=from_rdflib_term(t[1], term_dict),
                             object=from_rdflib_term(t[2], term_dict)) for t in cv['triples'])

        return Operator(name=name, type=cv.name, children=children_operators,
                        triples=triples, expressions=expressions)
    elif cv.name in {'Filter'}:
        triples = set()
        return Operator(name=name, type=cv.name, children=children_operators, triples=triples, expressions=expressions)
    else:
        assert False, 'Missing cv.name: ' + cv.name


def from_expression(e: Expr, term_dict: Dict) -> Set[Expression]:
    if e.name == 'RelationalExpression':
        expr_term = from_rdflib_term(e['expr'], term_dict)
        other = from_rdflib_term(e['other'], term_dict)
        expressions = {Expression(type=e.name, expr=expr_term, op=e['op'], other=other)}
        return expressions
    elif e.name == 'ConditionalAndExpression':
        e1 = e['expr']
        e2_exprs = list(map(lambda eo: from_expression(eo, term_dict), e['other']))
        return set.union(from_expression(e1, term_dict), *e2_exprs)
    else:
        assert False, 'Not Supported ' + str(e)


def from_rdflib_term(t, term_dict) -> Term:
    if t in term_dict:
        return term_dict[t]
    else:
        new_term = Term(rdflib_term=t)
        term_dict[t] = new_term
        return new_term


def find_children(q: CompValue) -> List[Tuple[Any, Any]]:
    children = [(p, q[p]) for p in q if p.startswith('p')]
    if None in children:
        print('Hello')
    return children
