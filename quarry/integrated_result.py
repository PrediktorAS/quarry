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

from typing import List, Set, Tuple

import pandas as pd
from rdflib.term import Variable, Literal

from .classes import Operator, Triple, TermConstraint, TimeSeriesQuery

join_ind = 0


def generate_select_result(op: Operator, static_df: pd.DataFrame, tsqs: List[TimeSeriesQuery]) -> pd.DataFrame:
    if op.type != 'SelectQuery':
        raise NotImplementedError('Only select queries are supported')

    df = static_df.copy()
    for c in op.children:
        df, tsqs = generate_result_delegate(c, df, tsqs)

    cols = [str(pv.rdflib_term) for pv in op.project_vars]

    assert len(tsqs) == 0, 'Should always be 0 after processing'

    return df[cols].copy()


def generate_result_delegate(op: Operator, df: pd.DataFrame, tsqs: List[TimeSeriesQuery]) -> Tuple[
    pd.DataFrame, List[TimeSeriesQuery]]:
    if op.type == 'LeftJoin':
        return generate_left_join(op, df, tsqs)
    elif op.type == 'BGP':
        return generate_bgp(op, df, tsqs)
    elif op.type == 'Filter':
        return generate_filter(op, df, tsqs)
    elif op.type == 'Project':
        # TODO: perhaps not so smart to skip a level here..
        return generate_result_delegate(next(c for c in op.children), df, tsqs)
    else:
        raise NotImplementedError(op.type)


def generate_left_join(op: Operator, df: pd.DataFrame, tsqs: List[TimeSeriesQuery]) -> Tuple[
    pd.DataFrame, List[TimeSeriesQuery]]:
    p1_child = [c for c in op.children if c.name == 'p1'][0]
    p2_child = [c for c in op.children if c.name == 'p2'][0]

    global join_ind
    join_col = 'my_special_join_col' + str(join_ind)
    df[join_col] = range(len(df))
    join_ind += 1

    df_lhs, tsqs_lhs = generate_result_delegate(op=p1_child, df=df, tsqs=tsqs)
    df_rhs, tsqs_rhs = generate_result_delegate(op=p2_child, df=df, tsqs=tsqs_lhs)  # TODO: Check if correct..
    rhs_newcols = [c for c in df_rhs.columns.values if c not in df_lhs.columns.values]
    df = df_lhs.set_index(join_col).join(df_rhs.set_index(join_col)[rhs_newcols], how='left')
    return df, tsqs_rhs


def generate_bgp(op: Operator, df: pd.DataFrame, tsqs: List[TimeSeriesQuery]) -> Tuple[
    pd.DataFrame, List[TimeSeriesQuery]]:
    df, tsqs = process_triples(op.triples, df=df, tsqs=tsqs)
    for c in op.children:
        generate_result_delegate(c, df, tsqs)
    df = filter_df(op, df)
    return df, tsqs


def generate_filter(op: Operator, df: pd.DataFrame, tsqs: List[TimeSeriesQuery]) -> Tuple[
    pd.DataFrame, List[TimeSeriesQuery]]:
    df, tsqs = process_triples(op.triples, df=df, tsqs=tsqs)
    for c in op.children:
        df, tsqs = generate_result_delegate(c, df, tsqs)
    df = filter_df(op, df)
    return df, tsqs


def process_triples(triples: Set[Triple], df: pd.DataFrame, tsqs: List[TimeSeriesQuery]) -> Tuple[
    pd.DataFrame, List[TimeSeriesQuery]]:
    tsqs = [t for t in tsqs]

    # First we join any time series data
    for t in triples:
        if TermConstraint.IS_EXTERNAL_UA_VARIABLE_VALUE in t.subject.constraints:
            for tsq in tsqs.copy():
                if tsq.variable_term == t.subject:
                    signal_id_col = str(t.subject.rdflib_term) + '_signal_id'
                    join_cols = [signal_id_col]

                    if tsq.timestamp_variable is not None:
                        timestamp_col = str(tsq.timestamp_variable.rdflib_term)
                        if timestamp_col in df.columns.values:
                            join_cols.append(timestamp_col)

                    df = df.set_index(join_cols, drop=False).join(tsq.df.set_index(join_cols), how='inner')
                    tsqs.remove(tsq)

    # for t in triples:
    #     # we need to filter out rows with missing value-columns that were made optional in case they were in
    #     # the time series database, but were not after all!
    #     if type(t.object.rdflib_term) == Variable:
    #         df = df.dropna(subset=[str(t.object.rdflib_term)])
    return df, tsqs


def filter_df(op: Operator, df: pd.DataFrame) -> pd.DataFrame:
    for e in op.expressions:
        if e.type == 'RelationalExpression':
            expr_colname = str(e.expr.rdflib_term)
            if expr_colname not in df.columns.values:
                raise ValueError(expr_colname + ' not found in dataframe')

            if type(e.other.rdflib_term) == Variable:
                other_colname = str(e.other.rdflib_term)
                if other_colname not in df.columns.values:
                    raise ValueError(other_colname + ' not found in dataframe')
                other_value = df[other_colname]
            elif type(e.other.rdflib_term) == Literal:
                other_value = e.other.rdflib_term.toPython()
            else:
                raise NotImplementedError(type(e.other.rdflib_term))

            if e.op == '>=':
                df = df[df[expr_colname] >= other_value]
            elif e.op == '>':
                df = df[df[expr_colname] > other_value]
            elif e.op == '<=':
                df = df[df[expr_colname] <= other_value]
            elif e.op == '<':
                df = df[df[expr_colname] < other_value]
            elif e.op == '=':
                df = df[df[expr_colname] == other_value]
            else:
                raise NotImplementedError('Operator in filter: ' + e.op)
        else:
            assert False, 'Unsupported expression type: ' + e.type

    return df
