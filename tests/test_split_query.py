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
import subprocess
import time
from io import StringIO

import opcua_tools
import pandas as pd
import psycopg2
import pytest
from SPARQLWrapper import SPARQLWrapper

import quarry
import swt_translator as oswt

PATH_HERE = os.path.dirname(__file__)


@pytest.fixture(scope='module')
def create_ttl():
    namespaces = ['http://opcfoundation.org/UA/', 'http://prediktor.com/paper_example',
                  'http://prediktor.com/RDS-OG-Fragment', 'http://prediktor.com/iec63131_fragment']
    parse_dict = opcua_tools.parse_xml_dir(PATH_HERE + '/input_data/split_query', namespaces=namespaces)

    params_dict = {'subclass_closure': True,
                   'subproperty_closure': True}

    signal_id_df = pd.DataFrame(
        {'NodeId': ['ns=1;i=27', 'ns=1;i=30', 'ns=1;i=33', 'ns=1;i=36', 'ns=1;i=39', 'ns=1;i=42'],
         'signal_id': [1, 2, 3, 4, 5, 6]})
    signal_id_df['NodeId'] = signal_id_df['NodeId'].map(opcua_tools.parse_nodeid)
    signal_id_df['ns'] = signal_id_df['NodeId'].map(lambda x: x.namespace)

    triples_dfs = oswt.build_swt(nodes=parse_dict['nodes'], references=parse_dict['references'],
                                 lookup_df=parse_dict['lookup_df'], signal_id_df=signal_id_df, params_dict=params_dict)

    output_file = PATH_HERE + '/expected/split_query/kb.ttl'
    g = oswt.build_instance_graph(triples_dfs=triples_dfs, namespaces=namespaces, params_dict=params_dict)

    g.serialize(destination=output_file, format='ttl', encoding='utf-8')
    return output_file


@pytest.fixture(scope='module')
def set_up_endpoint(create_ttl):
    containername = 'fusekidocker'

    print("Cleaning up potential old containers...")
    try:
        subprocess.run('docker stop ' + containername, shell=True)
        subprocess.run('docker rm ' + containername, shell=True)
    except Exception as e:
        print("Nothing to clean or cleaning failed.")

    print("Cleaning done.")

    cmd = f'docker run -d -p 3030:3030 -v {PATH_HERE + "/expected/split_query"}:/usr/share/data --name {containername} atomgraph/fuseki --file=/usr/share/data/kb.ttl /ds'
    print(cmd)
    subprocess.run(cmd, shell=True)
    time.sleep(10)
    yield

    subprocess.run('docker stop ' + containername, shell=True)
    subprocess.run('docker rm ' + containername, shell=True)


@pytest.fixture(scope='module')
def sparql_endpoint(set_up_endpoint) -> SPARQLWrapper:
    return SPARQLWrapper('http://localhost:3030/ds/sparql')


@pytest.fixture(scope='module')
def params():
    params_dict = {
        "host": 'localhost',
        "database": 'postgres',
        "user": 'postgres',
        "port": '5445',
        "password": 'hemelipasor'
    }
    return params_dict


@pytest.fixture(scope='module')
def postgresql(params):
    containername = 'postgresqlserver'
    volumename = 'postgresql'

    print("Cleaning up potential old containers...")
    try:
        subprocess.run('docker stop ' + containername, shell=True)
        subprocess.run('docker rm ' + containername, shell=True)
        subprocess.run('docker volume rm ' + volumename, shell=True)
    except Exception as e:
        print("Nothing to clean or cleaning failed.")

    print("Cleaning done.")

    cmd = f'docker run -d -v {volumename}:/var/lib/postgresql/data/ -e "POSTGRES_PASSWORD={params["password"]}" -p {params["port"]}:5432 --name {containername} postgres'
    subprocess.run(cmd, shell=True)
    time.sleep(10)
    yield

    subprocess.run('docker stop ' + containername, shell=True)
    subprocess.run('docker rm ' + containername, shell=True)
    subprocess.run('docker volume rm ' + volumename, shell=True)


@pytest.fixture(scope='module')
def timeseriesdata(postgresql, params):
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS TSDATA (ts TIMESTAMP, real_value REAL, signal_id INTEGER)')
    conn.commit()
    buffer = StringIO()
    df = pd.read_csv(PATH_HERE + '/input_data/split_query/signals.csv')
    df = df[['ts', 'real_value', 'signal_id']]
    df.to_csv(buffer, index=False, header=False, sep='|', quotechar="'")
    buffer.seek(0)
    cursor = conn.cursor()
    cursor.copy_from(buffer, 'TSDATA', sep="|")
    conn.commit()


def test_basic(sparql_endpoint, timeseriesdata):
    q = """
PREFIX rdsog: 
<http://prediktor.com/RDS-OG-Fragment#>
PREFIX opcua: 
<http://opcfoundation.org/UA/#>
PREFIX uahelpers: 
<http://prediktor.com/UA-helpers/#>
SELECT  ?cvalveName ?ts ?rv WHERE {
?injSystem a rdsog:InjectionSystemType.
?injSystem rdsog:functionalAspect+ ?cvalve. 
?cvalve a rdsog:LiquidControlValveType.
?cvalve opcua:displayName ?cvalveName.
?cvalve opcua:hierarchicalReferences ?cay.
?cay opcua:browseName "CA_Y".
?cay opcua:value ?cayValue.
?cayValue opcua:timestamp ?ts.
?cayValue opcua:realValue ?rv.
}
    """
    actual_df = quarry.execute_query(q, sparql_endpoint).reset_index(drop=True)
    #actual_df.to_csv(PATH_HERE + '/expected/split_query/basic2.csv', index=False)
    expected_df = pd.read_csv(PATH_HERE + '/expected/split_query/basic.csv')
    expected_df['ts'] = pd.to_datetime(expected_df['ts'])
    #ltx = actual_df.to_latex(index=False)
    #print(ltx)
    pd.testing.assert_frame_equal(actual_df, expected_df)

def test_basic_eu(sparql_endpoint, timeseriesdata):
    q = """
PREFIX rdsog: 
<http://prediktor.com/RDS-OG-Fragment#>
PREFIX opcua: 
<http://opcfoundation.org/UA/#>
PREFIX uahelpers: 
<http://prediktor.com/UA-helpers/#>
SELECT  ?cvalveName ?rv ?cayEU WHERE {
?injSystem a rdsog:InjectionSystemType.
?injSystem rdsog:functionalAspect+ ?cvalve. 
?cvalve a rdsog:LiquidControlValveType.
?cvalve opcua:displayName ?cvalveName.
?cvalve opcua:hierarchicalReferences ?cay.
?cay opcua:browseName "CA_Y".
?cay opcua:value ?cayValue.
?cayValue opcua:hasEngineeringUnit ?cayEU.
?cayValue opcua:realValue ?rv.
FILTER (?rv >= 0.07)
}
    """
    actual_df = quarry.execute_query(q, sparql_endpoint).reset_index(drop=True)
    #actual_df.to_csv(PATH_HERE + '/expected/split_query/basic_eu2.csv', index=False)
    expected_df = pd.read_csv(PATH_HERE + '/expected/split_query/basic_eu.csv')
    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_timestamp(sparql_endpoint, timeseriesdata):
    q = """
    PREFIX rdsog: 
    <http://prediktor.com/RDS-OG-Fragment#>
    PREFIX opcua: 
    <http://opcfoundation.org/UA/#>
    PREFIX uahelpers: 
    <http://prediktor.com/UA-helpers/#>
    SELECT  ?cvalveName ?cayValue ?ts ?rv ?cayEU WHERE {
        ?injSystem a rdsog:InjectionSystemType.
        ?injSystem rdsog:functionalAspect+ ?cvalve. 
        ?cvalve a rdsog:LiquidControlValveType.
        ?cvalve opcua:displayName ?cvalveName.
        ?cvalve opcua:hierarchicalReferences ?cay.
        ?cay opcua:browseName "CA_Y".
        ?cay opcua:value ?cayValue.
        ?cayValue opcua:hasEngineeringUnit ?cayEU.
        ?cayValue opcua:realValue ?rv.
        ?cayValue opcua:timestamp ?ts.
        FILTER (?rv < 0.06 && ?ts >= "2021-03-25T09:30:23.218499"^^xsd:dateTime)
        }
    """
    actual_df = quarry.execute_query(q, sparql_endpoint).reset_index(drop=True)
    #actual_df.to_csv(PATH_HERE + '/expected/split_query/timestamp2.csv', index=False)
    expected_df = pd.read_csv(PATH_HERE + '/expected/split_query/timestamp.csv')
    expected_df['ts'] = pd.to_datetime(expected_df['ts'])
    pd.testing.assert_frame_equal(actual_df, expected_df)

def test_timestamp_sync(sparql_endpoint, timeseriesdata):
    q = """
    PREFIX rdsog: 
    <http://prediktor.com/RDS-OG-Fragment#>
    PREFIX opcua: 
    <http://opcfoundation.org/UA/#>
    PREFIX uahelpers: 
    <http://prediktor.com/UA-helpers/#>
    SELECT  ?cvalveName ?ts ?y ?cayEU ?yr ?cayrEU WHERE {
        ?injSystem a rdsog:InjectionSystemType.
        ?injSystem rdsog:functionalAspect+ ?cvalve. 
        ?cvalve a rdsog:LiquidControlValveType.
        ?cvalve opcua:displayName ?cvalveName.
        ?cvalve opcua:hierarchicalReferences ?cay.
        ?cvalve opcua:hierarchicalReferences ?cayr.
        ?cay opcua:browseName "CA_Y".
        ?cayr opcua:browseName "CA_YR".
        ?cay opcua:value ?cayValue.
        ?cayr opcua:value ?cayrValue.
        ?cayValue opcua:hasEngineeringUnit ?cayEU.
        ?cayrValue opcua:hasEngineeringUnit ?cayrEU.
        ?cayValue opcua:realValue ?y.
        ?cayrValue opcua:realValue ?yr.
        ?cayValue opcua:timestamp ?ts.
        ?cayrValue opcua:timestamp ?ts.
        FILTER (?ts >= "2021-03-25T09:30:23.218499"^^xsd:dateTime)
        }
    """
    actual_df = quarry.execute_query(q, sparql_endpoint).reset_index(drop=True)
    #actual_df.to_csv(PATH_HERE + '/expected/split_query/timestamp_sync2.csv', index=False)
    expected_df = pd.read_csv(PATH_HERE + '/expected/split_query/timestamp_sync.csv')
    expected_df['ts'] = pd.to_datetime(expected_df['ts'])
    pd.testing.assert_frame_equal(actual_df, expected_df)
