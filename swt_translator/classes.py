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
