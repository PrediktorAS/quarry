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

    params_dict = {'subclass_closure': False,
                   'subproperty_closure': False}

    triples_dfs = oswt.build_swt(nodes=parse_dict['nodes'], references=parse_dict['references'],
                                 lookup_df=parse_dict['lookup_df'], params_dict=params_dict)

    output_file = PATH_HERE + '/expected/test_from_rdslike_vanilla/kb.ttl'
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

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike/basic_query.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/basic_query.csv')

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
    assert len(results) == 0, 'Should not do inheritance closure'


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
    assert len(results) == 0, 'Should not do inheritance closure'


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

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/attributes.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/attributes.csv')

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

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/functional_aspect_reference.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/functional_aspect_reference.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True).fillna('')
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_hierarchical_closure(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX iec61850ln: <http://opcfoundation.org/UA/IEC61850-7-4#>
    PREFIX rdslike: <http://prediktor.com/RDS-like-typelib/#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    PREFIX uahelpers: <http://prediktor.com/UA-helpers/#>
    SELECT ?nodea ?nodeb WHERE {
        ?nodea rdslike:functionalAspect+ ?nodeb .}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/hierarchical_closure2.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/test_from_rdslike_vanilla/hierarchical_closure.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)
