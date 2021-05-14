# Copyright 2021 Prediktor AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from rdflib.paths import MulPath
from rdflib.term import Variable, URIRef, Literal

from .classes import Operator, Triple, Term


def op_to_query(op: Operator):
    if op.type == 'SelectQuery':
        query = select_op_to_query(op)
    elif op.type == 'Project':
        query = project_op_to_query(op)
    elif op.type == 'LeftJoin':
        query = left_join_op_to_query(op)
    elif op.type == 'Join':
        query = join_op_to_query(op)
    elif op.type == 'Filter':
        query = filter_op_to_query(op)
    elif op.type == 'BGP':
        query = bgp_op_to_query(op)
    elif op.type == 'ToMultiSet':
        query = to_multiset_to_query(op)
    elif op.type == 'Distinct':
        query = distinct_to_query(op)
    else:
        raise NotImplementedError(op.type)
    return query


def project_op_to_query(op: Operator):
    query = ' '.join(map(lambda x: '?' + str(x.rdflib_term), op.project_vars)) + ' WHERE {\n'
    for c in op.children:
        query += op_to_query(c)
    query += '}'
    return query


def select_op_to_query(op: Operator):
    query = 'SELECT '
    for c in op.children:
        query += op_to_query(c)
    return query


def left_join_op_to_query(op: Operator):
    query = ''
    p1_child = [c for c in op.children if c.name == 'p1'][0]
    query += op_to_query(p1_child)
    p2_child = [c for c in op.children if c.name == 'p2'][0]
    query += 'OPTIONAL {\n'
    query += op_to_query(p2_child)
    query += '}\n'
    return query

def join_op_to_query(op: Operator):
    query = ''
    p1_child = [c for c in op.children if c.name == 'p1'][0]
    query += op_to_query(p1_child)
    p2_child = [c for c in op.children if c.name == 'p2'][0]
    query += '{\n'
    query += op_to_query(p2_child)
    query += '}\n'
    return query


def bgp_op_to_query(op: Operator):
    query = ''
    for t in op.triples:
        query += triple_string(t)
    if len(op.children) > 0:
        raise NotImplementedError
    return query


def filter_op_to_query(op: Operator):
    query = ''
    for c in op.children:
        query += op_to_query(c)
    return query


def mulpath_op_to_query(op: Operator):
    query = ''
    for c in op.children:
        query += op_to_query(c)
    return query

def distinct_to_query(op: Operator):
    query = 'SELECT DISTINCT '
    for c in op.children:
        query += op_to_query(c)
    return query


def to_multiset_to_query(op:Operator):
    query = ''
    for c in op.children:
        query += op_to_query(c)
    return query

def triple_string(triple: Triple):
    return term_string(triple.subject) + ' ' + term_string(triple.verb) + ' ' + term_string(triple.object) + '.\n'


def term_string(term: Term):
    if type(term.rdflib_term) == Variable:
        return '?' + str(term.rdflib_term)
    elif type(term.rdflib_term) == URIRef:
        return '<' + str(term.rdflib_term) + '>'
    elif type(term.rdflib_term) == MulPath:
        return '<' + str(term.rdflib_term.path) + '>' + str(term.rdflib_term.mod)
    elif type(term.rdflib_term) == Literal:
        if term.rdflib_term.datatype is None:
            return '"' + str(term.rdflib_term) + '"'
        else:
            raise NotImplementedError(term.rdflib_term.datatype)
    else:
        raise NotImplementedError(term)
