from typing import List, Dict, Any

import pandas as pd
from opcua_tools.ua_data_types import *
from rdflib import Graph, URIRef, RDF, RDFS, Literal, Namespace

from .classes import TriplesDfs
from .swt_builder import lowerfirst


def build_type_graph(triples_dfs: TriplesDfs, namespaces: List[str]) -> Graph:
    g = Graph()
    namespace_dict = create_namespace_dict(namespaces)
    type_uri_df = triples_dfs.type_uri_df.set_index('id')
    add_type_hierarchy(type_df=triples_dfs.type_df, type_uri_df=type_uri_df, namespace_dict=namespace_dict, g=g)
    add_reference_type_hierarchy(reference_type_df=triples_dfs.reference_type_df, type_uri_df=type_uri_df,
                                 namespace_dict=namespace_dict, g=g)

    return g


def build_instance_graph(triples_dfs: TriplesDfs, namespaces: List[str], params_dict: Dict[str, Any]) -> Graph:
    g = Graph()

    namespace_dict = create_namespace_dict(namespaces=namespaces)
    instance_uri_df = triples_dfs.instance_uri_df.set_index('id')
    type_uri_df = triples_dfs.type_uri_df.set_index('id')
    add_typing(typing_df=triples_dfs.typing_df, instance_uri_df=instance_uri_df, type_uri_df=type_uri_df, g=g,
               namespace_dict=namespace_dict)
    add_attributes(node_attributes_dfs=triples_dfs.node_attributes_dfs, instance_uri_df=instance_uri_df, g=g,
                   namespace_dict=namespace_dict)
    add_values(values_df=triples_dfs.values_df, instance_uri_df=instance_uri_df, type_uri_df=type_uri_df, g=g,
               namespace_dict=namespace_dict)

    add_references(references_df=triples_dfs.references_df,
                   instance_uri_df=instance_uri_df, type_uri_df=type_uri_df, g=g, namespace_dict=namespace_dict)

    add_is_external_variable(is_external_variable_df=triples_dfs.is_external_variable_df,
                             instance_uri_df=instance_uri_df, g=g,
                             namespace_dict=namespace_dict)

    if triples_dfs.signal_id_df is not None:
        add_signal_ids(signal_id_df=triples_dfs.signal_id_df, instance_uri_df=instance_uri_df, g=g,
                       namespace_dict=namespace_dict)

    return g


def create_namespace_dict(namespaces: List[str]) -> Dict[int, Namespace]:
    namespace_dict = {}
    for i, ns in enumerate(namespaces):
        namespace_dict[i] = Namespace(ns + '#')
    namespace_dict[-1] = Namespace('http://prediktor.com/UA-helpers/#')

    return namespace_dict


def attach_uri(df: pd.DataFrame, uri_df: pd.DataFrame, index_from_col: str, uri_col_rename: Optional[str]):
    if uri_col_rename is not None:
        uri_col = uri_col_rename
        uri_df = uri_df.rename(columns={'uri': uri_col_rename})
    else:
        uri_col = 'uri'
        uri_df = uri_df

    df = df.set_index(index_from_col, drop=False).join(uri_df[uri_col])
    return df


def add_svo_to_graph(svo: pd.DataFrame, g: Graph):
    tuples = svo[['subject', 'verb', 'object']].to_records(index=False).tolist()
    for t in tuples:
        g.add(t)


def create_uriref_in_namespace(df, ns_col, uri_col, namespace_dict):
    out = pd.Series(index=df.index)
    for ns in df[ns_col].unique():
        is_in_ns = df[ns_col] == ns
        ns_obj = namespace_dict[ns]
        out.loc[is_in_ns] = df.loc[is_in_ns, uri_col].map(lambda x: ns_obj[x])

    return out


def add_type_hierarchy(type_df: pd.DataFrame, type_uri_df: pd.DataFrame, namespace_dict: Dict[int, Namespace],
                       g: Graph):
    type_df = attach_uri(df=type_df, uri_df=type_uri_df, index_from_col='subtype', uri_col_rename='subtype_uri')
    type_df = attach_uri(df=type_df, uri_df=type_uri_df, index_from_col='supertype', uri_col_rename='supertype_uri')
    type_df['subject'] = create_uriref_in_namespace(type_df, 'subtype_ns', 'subtype_uri', namespace_dict)
    type_df['verb'] = RDFS.subClassOf
    type_df['object'] = create_uriref_in_namespace(type_df, 'supertype_ns', 'supertype_uri', namespace_dict)

    add_svo_to_graph(svo=type_df, g=g)


def add_reference_type_hierarchy(reference_type_df: pd.DataFrame, type_uri_df: pd.DataFrame,
                                 namespace_dict: Dict[int, Namespace], g: Graph):
    reference_type_df = attach_uri(df=reference_type_df, uri_df=type_uri_df, index_from_col='subtype',
                                   uri_col_rename='subtype_uri')
    reference_type_df = attach_uri(df=reference_type_df, uri_df=type_uri_df, index_from_col='supertype',
                                   uri_col_rename='supertype_uri')
    reference_type_df['subject'] = create_uriref_in_namespace(reference_type_df, 'subtype_ns', 'subtype_uri',
                                                              namespace_dict)
    reference_type_df['verb'] = RDFS.subPropertyOf
    reference_type_df['object'] = create_uriref_in_namespace(reference_type_df, 'supertype_ns', 'supertype_uri',
                                                             namespace_dict)

    add_svo_to_graph(svo=reference_type_df, g=g)


def add_typing(typing_df: pd.DataFrame, instance_uri_df: pd.DataFrame, type_uri_df: pd.DataFrame, g: Graph,
               namespace_dict: Dict[int, Namespace]):
    typing_df = attach_uri(df=typing_df, uri_df=instance_uri_df, index_from_col='id', uri_col_rename=None)
    typing_df = attach_uri(df=typing_df, uri_df=type_uri_df, index_from_col='type', uri_col_rename='type_uri')
    typing_df['subject'] = create_uriref_in_namespace(typing_df, 'ns', 'uri', namespace_dict)
    typing_df['verb'] = RDF.type
    typing_df['object'] = create_uriref_in_namespace(typing_df, 'type_ns', 'type_uri', namespace_dict)

    add_svo_to_graph(svo=typing_df, g=g)


def add_attributes(node_attributes_dfs: Dict[str, pd.DataFrame], instance_uri_df: pd.DataFrame, g: Graph,
                   namespace_dict: Dict[int, Namespace]):
    for a in node_attributes_dfs:
        df = node_attributes_dfs[a]
        df = attach_uri(df=df, uri_df=instance_uri_df, index_from_col='id', uri_col_rename=None)
        df['subject'] = create_uriref_in_namespace(df, 'ns', 'uri', namespace_dict)
        df['verb'] = namespace_dict[0][lowerfirst(a)]
        df['object'] = df['attribute']

        if a == 'BrowseNameNamespace':
            namespace_uriref_dict = {i: URIRef(namespace_dict[i]) for i in namespace_dict}
            df['object'] = df['object'].map(namespace_uriref_dict)
        else:
            df['object'] = df['object'].map(Literal)

        add_svo_to_graph(svo=df, g=g)


def add_values(values_df: pd.DataFrame, instance_uri_df: pd.DataFrame, type_uri_df: pd.DataFrame, g: Graph,
               namespace_dict: Dict[int, Namespace]):
    UA_VALUE = 'value'
    values_df = attach_uri(df=values_df, uri_df=instance_uri_df, index_from_col='id',
                           uri_col_rename=None)

    # hasValueReference
    values_df['subject'] = create_uriref_in_namespace(values_df, 'ns', 'uri', namespace_dict)
    values_df['verb'] = namespace_dict[0][UA_VALUE]
    values_df['object'] = values_df['subject'] + '_Value'
    add_svo_to_graph(svo=values_df, g=g)

    values_df['value_full_uri'] = values_df['object']

    # values_df = attach_uri(df=values_df, uri_df=type_uri_df, index_from_col='DataType', uri_col_rename='DataType_uri')
    # #value_typing
    # values_df['subject'] = values_df['value_full_uri']
    # values_df['verb'] = RDF.type
    # values_df['object'] = create_uriref_in_namespace(values_df, 'DataType_ns', 'DataType_uri', namespace_dict)
    # add_svo_to_graph(svo=values_df, g=g)

    # hasIntegerValue
    is_integer = values_df['Value'].map(lambda x: issubclass(type(x), UAInteger) and x.value is not None)
    integer_values_df = values_df[is_integer].copy()
    integer_values_df['subject'] = integer_values_df['value_full_uri']
    integer_values_df['verb'] = namespace_dict[0]['hasIntegerValue']
    integer_values_df['object'] = integer_values_df['Value'].map(lambda x: Literal(x.value))
    add_svo_to_graph(svo=integer_values_df, g=g)

    # hasFloatValue
    is_float = values_df['Value'].map(lambda x: issubclass(type(x), UAFloatingPoint) and x.value is not None)
    float_values_df = values_df[is_float].copy()
    float_values_df['subject'] = float_values_df['value_full_uri']
    float_values_df['verb'] = namespace_dict[0]['hasFloatValue']
    float_values_df['object'] = float_values_df['Value'].map(lambda x: Literal(x.value))
    add_svo_to_graph(svo=float_values_df, g=g)

    # hasStringValue
    is_string = values_df['Value'].map(lambda x: issubclass(type(x), UAString) and x.value is not None)
    string_values_df = values_df[is_string].copy()
    string_values_df['subject'] = string_values_df['value_full_uri']
    string_values_df['verb'] = namespace_dict[0]['hasStringValue']
    string_values_df['object'] = string_values_df['Value'].map(lambda x: Literal(x.value))
    add_svo_to_graph(svo=string_values_df, g=g)

    # EngineeringUnitsValue
    has_engineering_units = ~values_df['EngineeringUnitsValue'].isna()
    engineering_units_values_df = values_df[has_engineering_units].copy()
    engineering_units_values_df['subject'] = engineering_units_values_df['value_full_uri']
    engineering_units_values_df['verb'] = namespace_dict[0]['hasEngineeringUnit']
    engineering_units_values_df['object'] = engineering_units_values_df['EngineeringUnitsValue'].map(
        lambda x: Literal(x.display_name.text))
    add_svo_to_graph(svo=engineering_units_values_df, g=g)


def add_references(references_df: pd.DataFrame, instance_uri_df: pd.DataFrame, type_uri_df: pd.DataFrame, g: Graph,
                   namespace_dict: Dict[int, Namespace]):
    references_df = attach_uri(df=references_df, uri_df=instance_uri_df, index_from_col='src', uri_col_rename='src_uri')
    references_df = attach_uri(df=references_df, uri_df=instance_uri_df, index_from_col='trg', uri_col_rename='trg_uri')
    references_df = attach_uri(df=references_df, uri_df=type_uri_df, index_from_col='reference_type',
                               uri_col_rename='reference_type_uri')
    references_df['subject'] = create_uriref_in_namespace(references_df, 'src_ns', 'src_uri', namespace_dict)
    references_df['verb'] = create_uriref_in_namespace(references_df, 'reference_type_ns', 'reference_type_uri',
                                                       namespace_dict)
    references_df['object'] = create_uriref_in_namespace(references_df, 'trg_ns', 'trg_uri', namespace_dict)

    add_svo_to_graph(svo=references_df, g=g)


def add_is_external_variable(is_external_variable_df: pd.DataFrame, instance_uri_df: pd.DataFrame, g: Graph,
                             namespace_dict: Dict[int, Namespace]):
    is_external_variable_df = attach_uri(df=is_external_variable_df, uri_df=instance_uri_df, index_from_col='id',
                                         uri_col_rename='uri')
    is_external_variable_df['subject'] = create_uriref_in_namespace(is_external_variable_df, 'ns', 'uri',
                                                                    namespace_dict) + '_Value'
    is_external_variable_df['verb'] = namespace_dict[-1]['isExternalValue']
    is_external_variable_df['object'] = is_external_variable_df['is_external_variable'].map(Literal)
    add_svo_to_graph(svo=is_external_variable_df, g=g)


def add_signal_ids(signal_id_df: pd.DataFrame, instance_uri_df: pd.DataFrame, g: Graph,
                   namespace_dict: Dict[int, Namespace]):
    signal_id_df = attach_uri(df=signal_id_df, uri_df=instance_uri_df, index_from_col='id', uri_col_rename='uri')
    signal_id_df['subject'] = create_uriref_in_namespace(signal_id_df, 'ns', 'uri', namespace_dict) + '_Value'
    signal_id_df['verb'] = namespace_dict[-1]['signalId']
    signal_id_df['object'] = signal_id_df['signal_id'].map(Literal)

    add_svo_to_graph(svo=signal_id_df, g=g)
