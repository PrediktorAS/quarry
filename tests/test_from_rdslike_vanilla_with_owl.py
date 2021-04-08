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
import opcua_tools
import pandas as pd
import pytest
from owlrl import DeductiveClosure, OWLRL_Semantics
from rdflib import Graph

import swt_translator as oswt

PATH_HERE = os.path.dirname(__file__)


@pytest.fixture(scope='session')
def create_ttl():
    namespaces = ['http://opcfoundation.org/UA/', 'http://prediktor.com/sparql_testcase',
                  'http://prediktor.com/RDS-like-typelib/',
                  'http://opcfoundation.org/UA/IEC61850-7-3', 'http://opcfoundation.org/UA/IEC61850-7-4']
    parse_dict = opcua_tools.parse_xml_dir(PATH_HERE + '/input_data/test_from_rdslike', namespaces=namespaces)

    params_dict = {'subclass_closure': False,
                   'subproperty_closure': False}

    triples_dfs = oswt.build_swt(nodes=parse_dict['nodes'], references=parse_dict['references'],
                                 lookup_df=parse_dict['lookup_df'], params_dict=params_dict)

    output_file_ttl = PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/kb.ttl'
    output_file_owl = PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/ont.owl'

    g = oswt.build_instance_graph(triples_dfs=triples_dfs, namespaces=namespaces, params_dict=params_dict)
    g2 = oswt.build_type_graph(triples_dfs=triples_dfs, namespaces=namespaces)
    g2.serialize(destination=output_file_owl, format='pretty-xml', encoding='utf-8')
    g.serialize(destination=output_file_ttl, format='ttl', encoding='utf-8')
    return output_file_ttl, output_file_owl


@pytest.fixture
def set_up_rdflib(create_ttl):
    output_file_ttl, output_file_owl = create_ttl
    g = Graph()
    g.parse(source=output_file_ttl, format='turtle')
    g.parse(source=output_file_owl, format='xml')
    DeductiveClosure(OWLRL_Semantics).expand(g)
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

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/basic_query.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/basic_query.csv')

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

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/subclass_within_rds.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/subclass_within_rds.csv')

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
        ?nodea opcua:references ?nodeb.}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]

    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/subproperty_from_opcua.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/subproperty_from_opcua.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_attributes(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?node ?browseName ?browseNameNamespace ?displayName ?description ?nodeId ?nodeClass WHERE {
        ?node opcua:browseName ?browseName.
        ?node opcua:browseNameNamespace ?browseNameNamespace.
        ?node opcua:displayName ?displayName.
        ?node opcua:description ?description.
        ?node opcua:nodeId ?nodeId.
        ?node opcua:nodeClass ?nodeClass.
    }
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/attributes.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/attributes.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True).fillna('')
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_functional_aspect_reference(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX iec61850ln: <http://opcfoundation.org/UA/IEC61850-7-4#>
    PREFIX rdslike: <http://prediktor.com/RDS-like-typelib/#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?nodea ?nodeb WHERE {
        ?nodea rdslike:functionalAspect ?nodeb.}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/functional_aspect_reference.csv', index=False)

    df_expected = pd.read_csv(
        PATH_HERE + '/expected/test_from_rdslike_vanilla_with_owl/functional_aspect_reference.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True).fillna('')
    pd.testing.assert_frame_equal(df_actual, df_expected)
