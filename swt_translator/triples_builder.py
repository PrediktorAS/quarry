import logging
from typing import Dict, Any, Optional

import pandas as pd
from opcua_tools import subtypes_of_nodes, has_type_definition_references, \
    has_property_references, signal_variables

from .classes import TriplesDfs

logger = logging.getLogger(__name__)
cl = logging.StreamHandler()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
cl.setFormatter(formatter)
logger.addHandler(cl)


def build_triples_dfs(inst_nodes: pd.DataFrame, inst_references: pd.DataFrame, instance_uri_df: pd.DataFrame,
                      type_nodes: pd.DataFrame, type_references: pd.DataFrame, type_uri_df: pd.DataFrame,
                      params_dict: Dict[str, Any], signal_id_df: Optional[pd.DataFrame]) -> TriplesDfs:
    """Builds DataFrames of triples corresponding to ontologies for OPC UA type libraries and their instantiations

    :param inst_nodes: Nodes that are in namespaces containing no types
    :param inst_references:  References where either source or target are in a namespace containing no types
    :param instance_uri_df:  Lookup DataFrame for integer instance identifier to URI
    :param type_nodes: Nodes that are in namespaces containing types
    :param type_references: References where both source and target are in namespaces containing types
    :param type_uri_df:  Lookup DataFrame for integer instance identifier to URI
    :return:
    """

    type_df = create_type_df(type_nodes=type_nodes, type_references=type_references)
    reference_type_df = create_reference_type_df(type_nodes=type_nodes, type_references=type_references)
    typing_df = create_typing_df(inst_nodes=inst_nodes, inst_references=inst_references,
                                 type_nodes=type_nodes, type_df=type_df,
                                 subclass_closure=params_dict['subclass_closure'])
    node_attributes_dfs = create_node_attribute_dfs(inst_nodes=inst_nodes)
    values_df = create_values_df(inst_nodes=inst_nodes, inst_references=inst_references,
                                 type_nodes=type_nodes, type_references=type_references)

    is_external_variable_df = create_is_external_variable_df(inst_nodes=inst_nodes, inst_references=inst_references,
                                                             type_nodes=type_nodes, type_references=type_references)

    references_df = create_references_df(inst_references=inst_references, type_nodes=type_nodes,
                                         reference_type_df=reference_type_df,
                                         subproperty_closure=params_dict['subproperty_closure'])

    return TriplesDfs(type_df=type_df, reference_type_df=reference_type_df, typing_df=typing_df,
                      node_attributes_dfs=node_attributes_dfs, values_df=values_df,
                      is_external_variable_df=is_external_variable_df,
                      references_df=references_df, instance_uri_df=instance_uri_df, type_uri_df=type_uri_df,
                      signal_id_df=signal_id_df)


def create_type_df(type_nodes: pd.DataFrame, type_references: pd.DataFrame) -> pd.DataFrame:
    logger.info('Started creating type DataFrame')
    type_nodes_v_o = \
        type_nodes[(type_nodes['NodeClass'] == 'UAVariableType') | (type_nodes['NodeClass'] == 'UAObjectType')][
            'id'].to_list()
    subtypes = subtypes_of_nodes(type_nodes_v_o, type_nodes, type_references)
    subtypes = subtypes.rename(columns={'type': 'supertype'})[['supertype', 'subtype']]
    typenode_nses_sub = type_nodes[['id', 'ns']].rename(columns={'id': 'subtype', 'ns': 'subtype_ns'}).set_index(
        'subtype')
    typenode_nses_sup = type_nodes[['id', 'ns']].rename(columns={'id': 'supertype', 'ns': 'supertype_ns'}).set_index(
        'supertype')

    subtypes = subtypes.set_index('subtype').join(typenode_nses_sub).reset_index()
    subtypes = subtypes.set_index('supertype').join(typenode_nses_sup).reset_index()
    subtypes = subtypes[['supertype_ns', 'supertype', 'subtype_ns', 'subtype']].copy()
    logger.info('Finished creating instance DataFrame')
    return subtypes


def create_reference_type_df(type_nodes: pd.DataFrame, type_references: pd.DataFrame) -> pd.DataFrame:
    logger.info('Started creating reference type DataFrame')
    type_nodes_r = type_nodes[(type_nodes['NodeClass'] == 'UAReferenceType')]['id'].to_list()
    subtypes = subtypes_of_nodes(type_nodes_r, type_nodes, type_references)
    subtypes = subtypes.rename(columns={'type': 'supertype'})[['supertype', 'subtype']]
    typenode_nses_sub = type_nodes[['id', 'ns']].rename(
        columns={'id': 'subtype', 'ns': 'subtype_ns'}).set_index('subtype')
    typenode_nses_sup = type_nodes[['id', 'ns']].rename(
        columns={'id': 'supertype', 'ns': 'supertype_ns'}).set_index('supertype')

    subtypes = subtypes.set_index('subtype').join(typenode_nses_sub).reset_index()
    subtypes = subtypes.set_index('supertype').join(typenode_nses_sup).reset_index()
    subtypes = subtypes[['supertype_ns', 'supertype', 'subtype_ns', 'subtype']].copy()

    logger.info('Finished creating reference type DataFrame')
    return subtypes


def create_typing_df(inst_nodes: pd.DataFrame, inst_references: pd.DataFrame,
                     type_nodes: pd.DataFrame, type_df: pd.DataFrame,
                     subclass_closure: bool) -> pd.DataFrame:
    logger.info('Started creating typing DataFrame')
    # Methods do not have type definitions
    inst_nodes = inst_nodes[inst_nodes['NodeClass'] != 'UAMethod'].copy()
    inst_nodes = attach_type_definition(inst_nodes, inst_references, type_nodes)
    inst_nodes = inst_nodes[['id', 'ns', 'type', 'type_ns']]
    numna = inst_nodes.isna().sum().sum()
    if numna > 0:
        logger.warning('Typing DataFrame has ' + str(numna) + ' rows with na, dropping,  for instance: ' + str(
            inst_nodes[inst_nodes.isna()].iloc[0, :]))
        inst_nodes = inst_nodes.dropna().reset_index(drop=True)
        inst_nodes = inst_nodes.astype(int)

    if subclass_closure:
        inst_nodes = inst_nodes.drop(columns=['type_ns'])
        inst_nodes = inst_nodes.set_index('type').join(type_df.set_index('subtype')).reset_index(drop=True)
        inst_nodes = inst_nodes.rename(columns={'supertype': 'type', 'supertype_ns': 'type_ns'})
        inst_nodes = inst_nodes[['id', 'ns', 'type', 'type_ns']]

    logger.info('Finished creating typing DataFrame')
    return inst_nodes


def attach_type_definition(inst_nodes: pd.DataFrame, inst_references: pd.DataFrame,
                           type_nodes: pd.DataFrame) -> pd.DataFrame:
    typedef_references = has_type_definition_references(inst_references, type_nodes)
    inst_nodes = inst_nodes.set_index('id').join(typedef_references[['Src', 'Trg', 'Trg_ns']].set_index('Src'))
    inst_nodes = inst_nodes.reset_index().rename(columns={'index': 'id', 'Trg': 'type', 'Trg_ns': 'type_ns'})
    return inst_nodes


def create_references_df(inst_references: pd.DataFrame, type_nodes: pd.DataFrame, reference_type_df: pd.DataFrame,
                         subproperty_closure: bool) -> pd.DataFrame:
    """

    :param inst_references:
    :param type_nodes:
    :param reference_type_df:
    :return:
    """
    logger.info('Started creating reference DataFrame')
    type_nodes_idx = pd.Index(type_nodes['id'])
    src_inst_mask = ~pd.Index(inst_references['Src']).isin(type_nodes_idx)
    trg_inst_mask = ~pd.Index(inst_references['Trg']).isin(type_nodes_idx)
    only_inst_refs = inst_references[src_inst_mask & trg_inst_mask]
    only_inst_refs = only_inst_refs.rename(
        columns={'Src': 'src', 'Trg': 'trg', 'Src_ns': 'src_ns', 'Trg_ns': 'trg_ns', 'ReferenceType': 'reference_type'})
    type_nodes_tojoin = type_nodes[['id', 'ns']].rename(columns={'ns': 'reference_type_ns'}).set_index('id')
    only_inst_refs = only_inst_refs.set_index('reference_type', drop=False).join(type_nodes_tojoin)
    only_inst_refs = only_inst_refs[['src_ns', 'src', 'trg_ns', 'trg', 'reference_type_ns', 'reference_type']]

    if subproperty_closure:
        only_inst_refs = only_inst_refs.drop(columns=['reference_type_ns'])
        only_inst_refs = only_inst_refs.set_index('reference_type').join(
            reference_type_df.set_index('subtype')).reset_index(
            drop=True)
        only_inst_refs = only_inst_refs.rename(
            columns={'supertype': 'reference_type', 'supertype_ns': 'reference_type_ns'})
        only_inst_refs = only_inst_refs[['src_ns', 'src', 'trg_ns', 'trg', 'reference_type_ns', 'reference_type']]

    logger.info('Finished creating reference DataFrame')
    return only_inst_refs


def create_node_attribute_dfs(inst_nodes: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    logger.info('Started creating attributes DataFrame')
    attr_cols = ['NodeClass', 'NodeId', 'BrowseName', 'DisplayName', 'Description', 'BrowseNameNamespace']
    node_attributes = inst_nodes[['id', 'ns'] + attr_cols]
    attribute_dfs = dict()
    for a in attr_cols:
        attribute_dfs[a] = node_attributes[['id', 'ns', a]]
        attribute_dfs[a] = attribute_dfs[a].rename(columns={a: 'attribute'}, errors='raise')

    logger.info('Finished creating attributes DataFrame')
    return attribute_dfs


def create_values_df(inst_nodes: pd.DataFrame, inst_references: pd.DataFrame, type_nodes: pd.DataFrame,
                     type_references: pd.DataFrame) -> pd.DataFrame:
    logger.info('Started creating value DataFrame')
    variables = inst_nodes[inst_nodes['NodeClass'] == 'UAVariable'].set_index('id', drop=False)
    has_property = has_property_references(inst_references=inst_references, type_nodes=type_nodes,
                                           type_references=type_references).set_index('Trg', drop=False)
    has_property = has_property[['Src', 'Trg']].copy()

    engineering_units = variables[variables['BrowseName'] == 'EngineeringUnits'].set_index('id')
    has_property_eu = has_property[has_property.index.isin(engineering_units.index)].set_index('Src')
    engineering_units = engineering_units[['Value']].rename(columns={'Value': 'EngineeringUnitsValue'})

    values_df = variables.join(has_property_eu).set_index('Trg').join(engineering_units)

    eu_range = variables[variables['BrowseName'] == 'EURange'].set_index('id')
    has_property_eurange = has_property[has_property.index.isin(eu_range.index)].set_index('Src')
    eu_range = eu_range[['Value']].rename(columns={'Value': 'EURangeValue'})
    values_df = values_df.join(has_property_eurange).set_index('Trg').join(eu_range).reset_index()

    values_df = values_df[['ns', 'id', 'Value', 'EngineeringUnitsValue', 'EURangeValue']].copy()

    logger.info('Finished creating value DataFrame')
    return values_df


def create_is_external_variable_df(inst_nodes: pd.DataFrame, inst_references: pd.DataFrame, type_nodes: pd.DataFrame,
                                   type_references: pd.DataFrame) -> pd.DataFrame:
    logger.info('Started creating is_external_variable DataFrame')
    signals = signal_variables(instance_references=inst_references, type_nodes=type_nodes,
                               type_references=type_references)
    signals_index = pd.Index(signals.values)

    variables = inst_nodes.loc[inst_nodes['NodeClass'] == 'UAVariable', ['id', 'ns']].set_index('id', drop=False)
    is_signal = variables.index.isin(signals_index)

    variables.loc[is_signal, 'is_external_variable'] = True
    variables.loc[~is_signal, 'is_external_variable'] = False
    variables = variables.reset_index(drop=True)[['id', 'ns', 'is_external_variable']].copy()
    logger.info('Finished creating is_external_variable DataFrame')
    return variables
