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

import logging
from typing import List, Tuple, Dict, Any, Optional

import pandas as pd

from .triples_builder import build_triples_dfs

logger = logging.getLogger(__name__)
cl = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
cl.setFormatter(formatter)
logger.addHandler(cl)


def build_swt(nodes, references, lookup_df, params_dict: Dict[str, Any], signal_id_df: Optional[pd.DataFrame] = None):
    if signal_id_df is not None:
        uniques_index = pd.Index(lookup_df['uniques'].values)
        signal_id_df['id'] = uniques_index.get_indexer(pd.Index(signal_id_df['NodeId'])).astype(pd.Int32Dtype)

    type_namespaces = infer_type_namespaces(nodes)

    tuple_result = split_types_instances(nodes=nodes, references=references,
                                         type_namespaces=type_namespaces)

    inst_nodes, inst_references, type_nodes, type_references = tuple_result

    instance_uri_df, type_uri_df = create_uri_dfs(inst_nodes=inst_nodes, type_nodes=type_nodes)

    triples_dfs = build_triples_dfs(inst_nodes=inst_nodes, inst_references=inst_references,
                                    instance_uri_df=instance_uri_df,
                                    type_nodes=type_nodes, type_references=type_references, type_uri_df=type_uri_df,
                                    params_dict=params_dict, signal_id_df=signal_id_df)
    return triples_dfs


def infer_type_namespaces(nodes: pd.DataFrame):
    type_nodes = nodes[(nodes['NodeClass'] == 'UAObjectType') |
                       (nodes['NodeClass'] == 'UAReferenceType') |
                       (nodes['NodeClass'] == 'UAVariableType')]
    return list(type_nodes['ns'].unique())


def split_types_instances(nodes: pd.DataFrame, references: pd.DataFrame, type_namespaces: List[int]) -> Tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info('Started splitting types and instances')
    # Namespace 0 in OPC UA is always type namespace
    if 0 not in type_namespaces:
        type_namespaces.append(0)

    # We need namespace columns for source and target of references
    references = resolve_ns(references, 'Src', nodes)
    references = resolve_ns(references, 'Trg', nodes)

    type_src = False
    type_trg = False
    for ns in type_namespaces:
        type_src = type_src | (references['Src_ns'] == ns)
        type_trg = type_trg | (references['Trg_ns'] == ns)

    inst_references_mask = ~(type_src & type_trg)
    inst_references = references[inst_references_mask].copy()
    type_references = references[~inst_references_mask].copy()

    type_mask = False
    for ns in type_namespaces:
        type_mask = (nodes['ns'] == ns) | type_mask

    inst_nodes = nodes[~type_mask].copy()
    type_nodes = nodes[type_mask].copy()
    logger.info('Finished splitting types and instances')

    return inst_nodes, inst_references, type_nodes, type_references


def resolve_ns(df: pd.DataFrame, colname: str, nodes: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index(colname, drop=False).join(
        nodes[['id', 'ns']].set_index('id').rename(columns={'ns': colname + '_ns'}, errors='raise'))
    df = df.reset_index(drop=True)
    return df


def clean_nodeid(ser: pd.Series):
    return ser.str.replace('<', '_', regex=False).str.replace('>', '_', regex=False).str.replace('=', '_')


def create_uri_dfs(inst_nodes: pd.DataFrame, type_nodes: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    inst_nodes['uri'] = clean_nodeid(inst_nodes['NodeId'].map(lambda x: x.nodeid_type.value + '_' + x.value))

    # is_other = (~is_varobjtype) & (~is_reftype)

    is_varobjdatatype = (type_nodes['NodeClass'] == 'UADataType') | (type_nodes['NodeClass'] == 'UAObjectType') | (
                type_nodes['NodeClass'] == 'UAVariableType')
    is_reftype = (type_nodes['NodeClass'] == 'UAReferenceType')

    if is_varobjdatatype.sum() > 0:
        type_nodes.loc[is_varobjdatatype, 'uri'] = type_nodes.loc[is_varobjdatatype, 'DisplayName']

    if is_reftype.sum() > 0:
        type_nodes.loc[is_reftype, 'uri'] = type_nodes.loc[is_reftype, 'DisplayName'].map(lowerfirst)

    return inst_nodes[['id', 'ns', 'uri']].copy(), type_nodes[['id', 'ns', 'uri']].copy()


def lowerfirst(s: str) -> str:
    return s[0].lower() + s[1:]
