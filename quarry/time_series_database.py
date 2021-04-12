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

from dataclasses import dataclass, field
from typing import Optional, List
import pandas as pd
from .classes import Term, Expression
from abc import ABC, abstractmethod

@dataclass
class TimeSeriesQuery:
    variable_term: Term
    signal_ids: pd.Series
    df: Optional[pd.DataFrame] = field(default=None)
    timestamp_variable: Optional[Term] = field(default=None)
    data_variable: Optional[Term] = field(default=None)
    literal_expressions: List[Expression] = field(default_factory=list)
    datatype: Optional[str] = field(default=None)


class TimeSeriesDatabase(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def execute_query(self, tsq: TimeSeriesQuery) -> pd.DataFrame:
        pass