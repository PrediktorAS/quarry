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

from quarry.time_series_database import TimeSeriesDatabase, TimeSeriesQuery
import psycopg2
import pandas as pd


class SQLTimeSeriesDatabase(TimeSeriesDatabase):
    def __init__(self, params_dict):
        self.conn = psycopg2.connect(**params_dict)
        super().__init__()

    def execute_query(self, tsq: TimeSeriesQuery) -> pd.DataFrame:

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

        df = pd.read_sql(query, self.conn)

        rename_dict = {}
        rename_dict['signal_id'] = str(tsq.variable_term.rdflib_term) + '_signal_id'
        if tsq.data_variable is not None:
            rename_dict[tsq.datatype + '_value'] = str(tsq.data_variable.rdflib_term)

        if tsq.timestamp_variable is not None:
            rename_dict['ts'] = str(tsq.timestamp_variable.rdflib_term)

        df = df.rename(columns=rename_dict, errors='raise')
        return df
