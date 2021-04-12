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
    output_file = PATH_HERE + '/expected/translate_paper_example_all_closures/kb.ttl'

    swtt.translate(xml_dir=PATH_HERE + '/input_data/translate_paper_example', namespaces=namespaces,
                   output_ttl_file=output_file, subproperty_closure=True, subclass_closure=True)
    return output_file


@pytest.fixture
def set_up_rdflib(create_ttl):
    g = Graph()
    g.parse(source=create_ttl, format='turtle')
    return g


def test_functional_hierarchy_query(set_up_rdflib):
    g = set_up_rdflib
    q = """
    PREFIX rdsog: 
    <http://prediktor.com/RDS-OG-Fragment#>
    PREFIX opcua: 
    <http://opcfoundation.org/UA/#>
    PREFIX uahelpers: 
    <http://prediktor.com/UA-helpers/#>
    SELECT  ?siteName  ?cvalveName 
            ?cayNodeId ?cayEU WHERE {
        ?site a rdsog:SiteType.
        ?site opcua:displayName ?siteName.
        ?site rdsog:functionalAspect ?injSystem.
        ?injSystem a rdsog:InjectionSystemType.
        ?injSystem uahelpers:funcHierarchy ?cvalve. 
        ?cvalve a rdsog:LiquidControlValveType.
        ?cvalve opcua:displayName ?cvalveName.
        ?cvalve opcua:hierarchicalReferences ?cay.
        ?cay opcua:browseName "CA_Y".
        ?cay opcua:value ?cayValue.
        ?cayValue opcua:hasEngineeringUnit ?cayEU.
        ?cay opcua:nodeId ?cayNodeId.
        }
    """
    res = g.query(q)
    results = [tuple(map(str, r)) for r in res]
    df_actual = pd.DataFrame(results, columns=list(map(str, res.vars)))
    ltx = df_actual.to_latex(index=False)
    df_actual.to_csv(PATH_HERE + '/expected/translate_paper_example_all_closures/functional_hierarchy.csv', index=False)

    df_expected = pd.read_csv(PATH_HERE + '/expected/translate_paper_example_all_closures/functional_hierarchy.csv')

    df_actual = df_actual.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=df_actual.columns.values.tolist()).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_actual, df_expected)
