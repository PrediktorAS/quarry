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

from .graph_builder import build_instance_graph
from .graph_builder import build_type_graph
from .swt_builder import build_swt
from typing import List, Optional
import pandas as pd
from opcua_tools import parse_xml_dir, parse_nodeid


def translate(xml_dir: str, namespaces: List[str], output_ttl_file: str, output_owl_file: Optional[str] = None,
              subclass_closure: bool = False, subproperty_closure: bool = False,
              signal_id_csv: Optional[str] = None):
    parse_dict = parse_xml_dir(xmldir=xml_dir, namespaces=namespaces)
    params_dict = {'subclass_closure': subclass_closure,
                   'subproperty_closure': subproperty_closure}
    if signal_id_csv is not None:
        signal_id_df = pd.read_csv(signal_id_csv)
        signal_id_df['NodeId'] = signal_id_df['NodeId'].map(parse_nodeid)
        signal_id_df['ns'] = signal_id_df['NodeId'].map(lambda x: x.namespace)
        signal_id_df['signal_id'] = signal_id_df['signal_id'].astype(pd.Int32Dtype())
    else:
        signal_id_df = None

    triples_dfs = build_swt(nodes=parse_dict['nodes'], references=parse_dict['references'],
                            lookup_df=parse_dict['lookup_df'], signal_id_df=signal_id_df, params_dict=params_dict)

    g = build_instance_graph(triples_dfs=triples_dfs, namespaces=namespaces, params_dict=params_dict)
    g.serialize(destination=output_ttl_file, format='ttl', encoding='utf-8')

    if output_owl_file is not None:
        g2 = build_type_graph(triples_dfs=triples_dfs, namespaces=namespaces)
        g2.serialize(destination=output_owl_file, format='pretty-xml', encoding='utf-8')
