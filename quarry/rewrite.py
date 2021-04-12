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

import copy
from typing import List

import pandas as pd
from rdflib.term import URIRef, Variable, Literal

from .classes import Operator, TermConstraint, Triple, Term
from .time_series_database import TimeSeriesQuery
from .type_inference import REAL_VALUE_VERB, BOOL_VALUE_VERB, INT_VALUE_VERB, STRING_VALUE_VERB, TIMESTAMP_VERB

IS_EXTERNAL_VALUE_PROPERTY_URI = 'http://prediktor.com/UA-helpers/#isExternalValue'
SIGNAL_ID_PROPERTY = 'http://prediktor.com/UA-helpers/#signalId'


def rewrite_deepcopy_for_sparql_engine(op: Operator):
    rewritten_children = set()
    new_project_vars = set()

    for c in op.children:
        c_rw, cw_new_project_vars = rewrite_deepcopy_for_sparql_engine(c)
        rewritten_children.add(c_rw)
        new_project_vars = new_project_vars.union(cw_new_project_vars)

    optional_triples = set()
    mandatory_triples = set()

    for trip in list(op.triples):
        if TermConstraint.IS_EXTERNAL_UA_VARIABLE_VALUE in trip.subject.constraints and \
                (TermConstraint.IS_EXTERNAL_DATA_VALUE in trip.object.constraints or
                 TermConstraint.IS_TIMESTAMP in trip.object.constraints):
            # Effecively remove these triples
            signal_id_property = Term(rdflib_term=URIRef(SIGNAL_ID_PROPERTY))
            signal_id_variable = Term(rdflib_term=Variable(str(trip.subject.rdflib_term) + '_signal_id'))
            mandatory_triples.add(Triple(
                subject=copy.deepcopy(trip.subject),
                verb=signal_id_property,
                object=signal_id_variable
            ))
            new_project_vars.add(signal_id_variable)
        elif (TermConstraint.IS_UA_VARIABLE_VALUE in trip.subject.constraints) and \
                (TermConstraint.IS_DATA_VALUE in trip.object.constraints):

            if (TermConstraint.IS_DATA_VALUE in trip.object.constraints):
                optional_triples.add(copy.deepcopy(trip))
            else:
                mandatory_triples.add(copy.deepcopy(trip))

            is_external_variable_property = Term(rdflib_term=URIRef(IS_EXTERNAL_VALUE_PROPERTY_URI))
            is_external_variable = Term(rdflib_term=Variable(str(trip.subject.rdflib_term) + '_is_ext_var'))
            mandatory_triples.add(Triple(subject=copy.deepcopy(trip.subject),
                                         verb=is_external_variable_property,
                                         object=is_external_variable))
            signal_id_property = Term(rdflib_term=URIRef(SIGNAL_ID_PROPERTY))
            signal_id_variable = Term(rdflib_term=Variable(str(trip.subject.rdflib_term) + '_signal_id'))
            optional_triples.add(Triple(
                subject=copy.deepcopy(trip.subject),
                verb=signal_id_property,
                object=signal_id_variable
            ))
            new_project_vars.add(signal_id_variable)
            new_project_vars.add(is_external_variable)
        else:
            mandatory_triples.add(copy.deepcopy(trip))

    new_op = Operator(type=op.type, name=op.name, triples=mandatory_triples, children=rewritten_children)
    if op.type == 'SelectQuery':
        filtered_project_vars = [p for p in op.project_vars if
                                 len(p.constraints.intersection({TermConstraint.IS_TIMESTAMP,
                                                                 TermConstraint.IS_EXTERNAL_DATA_VALUE})) == 0]
        new_op.project_vars = [copy.deepcopy(p) for p in filtered_project_vars] + list(new_project_vars)

    if len(optional_triples) > 0:
        join_expression_name = new_op.name
        new_op.name = 'p1'
        optional_expression = generate_optional_expression(src_op=new_op, root_name=join_expression_name,
                                                           triplist=list(optional_triples))

        return optional_expression, new_project_vars
    else:
        return new_op, new_project_vars


def generate_optional_expression(src_op: Operator, root_name: str, triplist: List[Triple]):
    rhs = Operator(type='BGP', name='p2', triples={triplist[0]}, children=set())
    if len(triplist) == 1:
        root_op = Operator(type='LeftJoin', name=root_name, triples=set(), children={src_op, rhs})
        return root_op
    else:
        lhs = generate_optional_expression(src_op=src_op, root_name='p1', triplist=triplist[1:])
        return Operator(type='LeftJoin', name=root_name, triples=set(), children={lhs, rhs})


def generate_time_series_queries(op: Operator, df: pd.DataFrame, time_series_queries, timestamp_to_query,
                                 data_to_query):
    for c in op.children:
        generate_time_series_queries(c, df, time_series_queries, timestamp_to_query, data_to_query)

    for t in op.triples:
        if (TermConstraint.IS_EXTERNAL_UA_VARIABLE_VALUE in t.subject.constraints):
            if t.subject not in time_series_queries:
                ser = df[str(t.subject.rdflib_term) + '_signal_id']
                time_series_queries[t.subject] = TimeSeriesQuery(t.subject, ser)

            q = time_series_queries[t.subject]

            vrb = t.verb.rdflib_term.toPython()
            if vrb == TIMESTAMP_VERB:
                if type(t.object.rdflib_term) == Variable:
                    if t.object not in timestamp_to_query:
                        timestamp_to_query[t.object] = []
                    timestamp_to_query[t.object].append(q)
                    q.timestamp_variable = t.object
                else:
                    raise NotImplementedError('Timestamp not variable.. ')
            elif vrb == REAL_VALUE_VERB:
                q.datatype = 'real'
                if type(t.object.rdflib_term) == Variable:
                    data_to_query[t.object] = q
                    q.data_variable = t.object
            elif vrb == INT_VALUE_VERB:
                q.datatype = 'int'
                if type(t.object.rdflib_term) == Variable:
                    data_to_query[t.object] = q
                    q.data_variable = t.object
            elif vrb == STRING_VALUE_VERB:
                q.datatype = 'str'
                if type(t.object.rdflib_term) == Variable:
                    data_to_query[t.object] = q
                    q.data_variable = t.object
            elif vrb == BOOL_VALUE_VERB:
                q.datatype = 'bool'
                if type(t.object.rdflib_term) == Variable:
                    data_to_query[t.object] = q
                    q.data_variable = t.object

    for e in op.expressions:
        if type(e.expr.rdflib_term) == Variable:
            if e.expr in timestamp_to_query:
                if type(e.other.rdflib_term) == Literal:
                    for q in timestamp_to_query[e.expr]:
                        q.literal_expressions.append(e)
                else:
                    raise NotImplementedError(e)
            elif e.expr in data_to_query:
                if type(e.other.rdflib_term) == Literal:
                    data_to_query[e.expr].literal_expressions.append(e)
        if type(e.other.rdflib_term) == Variable:
            if e.other in timestamp_to_query:
                if type(e.expr.rdflib_term) == Literal:
                    for q in timestamp_to_query[e.other]:
                        q.literal_expressions.append(e)
                else:
                    raise NotImplementedError(e)
            elif e.other in data_to_query:
                if type(e.expr.rdflib_term) == Literal:
                    data_to_query[e.other].literal_expressions.append(e)
