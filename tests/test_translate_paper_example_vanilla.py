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

import swt_translator as swtt

PATH_HERE = os.path.dirname(__file__)


@pytest.fixture(scope='session')
def create_ttl():
    namespaces = ['http://opcfoundation.org/UA/', 'http://prediktor.com/paper_example',
                  'http://prediktor.com/RDS-OG-Fragment', 'http://prediktor.com/iec63131_fragment']
    output_file = PATH_HERE + '/expected/translate_paper_example_vanilla/kb.ttl'
    swtt.translate(xml_dir=PATH_HERE + '/input_data/translate_paper_example', namespaces=namespaces,
                   output_ttl_file=output_file)
    return output_file


@pytest.fixture
def set_up_rdflib(create_ttl):
    g = Graph()
    g.parse(source=create_ttl, format='turtle')
    return g


def test_basic_query(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX ex: <http://prediktor.com/paper_example#>
    PREFIX scd: <http://prediktor.com/iec63131_fragment#>
    PREFIX rdsog: <http://prediktor.com/RDS-OG-Fragment#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?node ?value WHERE {
        ?node opcua:hasProperty ?pname.
        ?pname opcua:displayName "ProductAspectName".
        ?pname opcua:hasValue ?pvalue.
        ?pvalue opcua:hasStringValue ?value}
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/paper_example/basic_query.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/translate_paper_example_vanilla/basic_query.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_engineering_units(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX ex: <http://prediktor.com/paper_example#>
    PREFIX scd: <http://prediktor.com/iec63131_fragment#>
    PREFIX rdsog: <http://prediktor.com/RDS-OG-Fragment#>
    PREFIX opcua: <http://opcfoundation.org/UA/#>
    SELECT ?node ?name ?eu WHERE {
        ?node a opcua:AnalogItemType.
        ?node opcua:displayName ?name.
        ?node opcua:value ?value.
        ?value opcua:hasEngineeringUnit ?eu.
        }
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))

    # df_actual.to_csv(PATH_HERE + '/expected/translate_paper_example_vanilla/engineering_units.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/translate_paper_example_vanilla/engineering_units.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)
