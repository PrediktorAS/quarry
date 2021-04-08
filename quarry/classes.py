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
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Set, Union, Optional

import pandas as pd
import psycopg2
from rdflib.paths import MulPath
from rdflib.term import Variable, URIRef, Literal


class TermConstraint(Enum):
    IS_UA_VARIABLE_VALUE = 1
    IS_EXTERNAL_UA_VARIABLE_VALUE = 2
    IS_TIMESTAMP = 3
    IS_DATA_VALUE = 4
    IS_EXTERNAL_DATA_VALUE = 5


@dataclass
class Term:
    rdflib_term: Union[Variable, URIRef, Literal, MulPath]
    constraints: Set[TermConstraint] = field(default_factory=set)

    def __hash__(self):
        return self.rdflib_term.__hash__()

    def __deepcopy__(self, memo):
        return Term(rdflib_term=copy.deepcopy(self.rdflib_term), constraints=copy.deepcopy(self.constraints))


@dataclass
class Triple:
    subject: Term
    verb: Term
    object: Term

    def __hash__(self):
        return hash((self.subject.__hash__(), self.verb.__hash__(), self.object.__hash__()))

    def __deepcopy__(self, memo):
        return Triple(subject=copy.deepcopy(self.subject), verb=copy.deepcopy(self.verb),
                      object=copy.deepcopy(self.object))


@dataclass
class Expression:
    type: str
    expr: Term
    op: str
    other: Term

    def __hash__(self):
        return hash((self.type, self.expr, self.op, self.other))


@dataclass
class Operator:
    type: str
    name: str
    project_vars: List[Term] = field(init=False)
    triples: Set[Triple]
    order_by: Set[Term] = field(init=False)
    children: Set['Operator']
    expressions: Set[Expression] = field(default_factory=set)
    guid: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __hash__(self):
        return hash((self.guid))


@dataclass
class TimeSeriesQuery:
    variable_term: Term
    signal_ids: pd.Series
    df: Optional[pd.DataFrame] = field(default=None)
    timestamp_variable: Optional[Term] = field(default=None)
    data_variable: Optional[Term] = field(default=None)
    literal_expressions: List[Expression] = field(default_factory=list)
    datatype: Optional[str] = field(default=None)


class TimeSeriesDatabase:
    def execute_query(self, tsq: TimeSeriesQuery) -> pd.DataFrame:
        pass


class SQLTimeSeriesDatabase(TimeSeriesDatabase):
    def execute_query(self, tsq: TimeSeriesQuery) -> pd.DataFrame:
        params_dict = {
            "host": 'localhost',
            "database": 'postgres',
            "user": 'postgres',
            "port": '5445',
            "password": 'hemelipasor'
        }
        conn = psycopg2.connect(**params_dict)

        cols = ['signal_id']
        if tsq.timestamp_variable is not None:
            cols.append('ts')
        if tsq.datatype == 'str':
            cols.append('str_value')
        elif tsq.datatype == 'real':
            cols.append('real_value')
        elif tsq.datatype == 'int':
            cols.append('int_value')
        elif tsq.datatype == 'bool':
            cols.append('bool_value')

        query = f"""SELECT {', '.join(map(lambda x: 't.' + x, cols))} FROM TSDATA t WHERE t.signal_id in ({','.join(map(str, tsq.signal_ids.to_list()))});"""

        print(query)

        df = pd.read_sql(query, conn)
        rename_dict = {}
        rename_dict['signal_id'] = str(tsq.variable_term.rdflib_term) + '_signal_id'
        if tsq.data_variable is not None:
            rename_dict[tsq.datatype + '_value'] = str(tsq.data_variable.rdflib_term)

        if tsq.timestamp_variable is not None:
            rename_dict['ts'] = str(tsq.timestamp_variable.rdflib_term)

        df = df.rename(columns=rename_dict, errors='raise')
        return df
