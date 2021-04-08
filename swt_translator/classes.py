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

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd


@dataclass
class TriplesDfs:
    """
    The class holds triples for constructing OPC UA knowledge base.
    It is normalized: identifiers are numeric, and there are lookup tables to find string URIs.
    """
    type_df: pd.DataFrame
    reference_type_df: pd.DataFrame
    typing_df: pd.DataFrame
    node_attributes_dfs: Dict[str, pd.DataFrame]
    values_df: Optional[pd.DataFrame]
    is_external_variable_df: pd.DataFrame
    references_df: pd.DataFrame
    instance_uri_df: pd.DataFrame
    type_uri_df: pd.DataFrame
    signal_id_df: Optional[pd.DataFrame]
