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

import os

import pandas as pd
import pytest
from rdflib import Graph
import opcua_tools
import swt_translator as oswt

PATH_HERE = os.path.dirname(__file__)


@pytest.fixture(scope='session')
def create_ttl():
    namespaces = ['http://opcfoundation.org/UA/', 'http://prediktor.com/sparql_testcase',
                  'http://prediktor.com/RDS-like-typelib/',
                  'http://opcfoundation.org/UA/IEC61850-7-3', 'http://opcfoundation.org/UA/IEC61850-7-4']
    parse_dict = opcua_tools.parse_xml_dir(PATH_HERE + '/input_data/test_from_rdslike', namespaces=namespaces)

    params_dict = {'subclass_closure': True,
                   'subproperty_closure': True}

    triples_dfs = oswt.build_swt(nodes=parse_dict['nodes'], references=parse_dict['references'],
                                 lookup_df=parse_dict['lookup_df'], params_dict=params_dict)

    output_file = PATH_HERE + '/expected/test_from_rdslike_all_closures/kb.ttl'
    g = oswt.build_instance_graph(triples_dfs=triples_dfs, namespaces=namespaces, params_dict=params_dict)
    g.serialize(destination=output_file, format='ttl', encoding='utf-8')

    return output_file


@pytest.fixture
def set_up_rdflib(create_ttl):
    g = Graph()
    g.parse(source=create_ttl, format='turtle')
    return g


def test_basic_query(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX iec61850ln: <http://opcfoundation.org/UA/IEC61850-7-4#>
    PREFIX rdslike: <http://prediktor.com/RDS-like-typelib/#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?node ?name WHERE {
        ?node rdslike:hasLogicalNode ?svbr.
        ?svbr a iec61850ln:SVBR.
        ?node opcua:displayName ?name.
    }
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/basic_query.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/basic_query.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_subclass_within_rds(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX iec61850ln: <http://opcfoundation.org/UA/IEC61850-7-4#>
    PREFIX rdslike: <http://prediktor.com/RDS-like-typelib/#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?node WHERE {
        ?node a rdslike:TopSystemType.}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/subclass_within_rds.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/subclass_within_rds.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_subclass_from_opcua(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX iec61850ln: <http://opcfoundation.org/UA/IEC61850-7-4#>
    PREFIX rdslike: <http://prediktor.com/RDS-like-typelib/#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?node WHERE {
        ?node a opcua:BaseObjectType .}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/subclass_from_opcua.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/subclass_from_opcua.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_subproperty_from_opcua(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX iec61850ln: <http://opcfoundation.org/UA/IEC61850-7-4#>
    PREFIX rdslike: <http://prediktor.com/RDS-like-typelib/#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?nodea ?nodeb WHERE {
        ?nodea opcua:references ?nodeb .}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/subproperty_from_opcua.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_all_closures/subproperty_from_opcua.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)
