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

from typing import Dict, Set, List

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.plugins.sparql import prepareQuery
from rdflib.term import Variable

from .algebra_utils import from_rdflib_sparqlquery
from .classes import Operator, Term, TermConstraint, TimeSeriesQuery, SQLTimeSeriesDatabase
from .integrated_result import generate_select_result
from .query_generator import op_to_query
from .rewrite import rewrite_deepcopy_for_sparql_engine, generate_time_series_queries
from .type_inference import infer_types


def execute_query(sparql: str, sparql_endpoint: SPARQLWrapper) -> pd.DataFrame:
    query = prepareQuery(sparql)
    op = from_rdflib_sparqlquery(query)
    infer_types(op)
    infer_types(op)
    op_for_sparql, _ = rewrite_deepcopy_for_sparql_engine(op)
    model_sparql = op_to_query(op_for_sparql)
    sparql_endpoint.setQuery(model_sparql)
    sparql_endpoint.setReturnFormat(JSON)
    static_dict = sparql_endpoint.query().convert()
    static_df = convert_result_to_dataframe(res_dict=static_dict)

    is_ext = {c.replace('_is_ext_var', '') for c in static_df.columns.values if
              c.endswith('_is_ext_var') and static_df[c].any()}

    update_operator_with_result(op, is_ext)
    time_series_queries = {}
    generate_time_series_queries(op, static_df, time_series_queries, {}, {})
    tsqs = execute_time_series_queries(time_series_queries)

    dropmore = [c for c in static_df.columns.values if c.endswith('_is_ext_var')]
    dropvars = [str(tsq.data_variable.rdflib_term) for tsq in tsqs if tsq.data_variable is not None]
    filtered_dropcols = [c for c in dropmore + dropvars if c in static_df.columns.values]
    static_df = static_df.drop(columns=filtered_dropcols)

    result_df = generate_select_result(op, static_df, tsqs)

    return result_df


def execute_time_series_queries(time_series_queries: Dict[Term, TimeSeriesQuery]) -> List[TimeSeriesQuery]:
    sqldb = SQLTimeSeriesDatabase()
    tsqs = []

    for trm in time_series_queries:
        tsq = time_series_queries[trm]
        tsq_df = sqldb.execute_query(tsq)
        tsq.df = tsq_df
        tsqs.append(tsq)

    return tsqs


def convert_result_to_dataframe(res_dict: Dict):
    res_df = pd.DataFrame.from_records(res_dict['results']['bindings'])
    for c in res_df.columns.values:
        res_df[c] = res_df[c].map(lambda x: x['value'] if x is not None else None)
    for c in res_dict['head']['vars']:
        if c not in res_df.columns.values:
            res_df[c] = pd.NA
    signalcols = [c for c in res_df.columns.values if c.endswith('_signal_id')]
    for s in signalcols:
        res_df[s] = res_df[s].map(lambda x: int(x) if x is not None else None)
        res_df[s] = res_df[s].astype(pd.Int32Dtype())

    return res_df


def update_operator_with_result(op: Operator, is_ext: Set[str]):
    for t in op.triples:
        update_term_is_ext(t.subject, is_ext)
        update_term_is_ext(t.object, is_ext)
    for c in op.children:
        update_operator_with_result(c, is_ext)


def update_term_is_ext(t: Term, is_ext: Set[str]):
    if type(t.rdflib_term) == Variable:
        vname = str(t.rdflib_term)
        if vname in is_ext:
            t.constraints.add(TermConstraint.IS_EXTERNAL_UA_VARIABLE_VALUE)
