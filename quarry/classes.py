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

import copy
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Set, Union

from rdflib.paths import MulPath
from rdflib.term import Variable, URIRef, Literal


class TermConstraint(Enum):
    IS_UA_VARIABLE_VALUE = 1
    IS_EXTERNAL_UA_VARIABLE_VALUE = 2
    IS_TIMESTAMP = 3
    IS_DATA_VALUE = 4
    IS_EXTERNAL_DATA_VALUE = 5


@dataclass
class Term:
    rdflib_term: Union[Variable, URIRef, Literal, MulPath]
    constraints: Set[TermConstraint] = field(default_factory=set)

    def __hash__(self):
        return self.rdflib_term.__hash__()

    def __deepcopy__(self, memo):
        return Term(rdflib_term=copy.deepcopy(self.rdflib_term), constraints=copy.deepcopy(self.constraints))


@dataclass
class Triple:
    subject: Term
    verb: Term
    object: Term

    def __hash__(self):
        return hash((self.subject.__hash__(), self.verb.__hash__(), self.object.__hash__()))

    def __deepcopy__(self, memo):
        return Triple(subject=copy.deepcopy(self.subject), verb=copy.deepcopy(self.verb),
                      object=copy.deepcopy(self.object))


@dataclass
class Expression:
    type: str
    expr: Term
    op: str
    other: Term

    def __hash__(self):
        return hash((self.type, self.expr, self.op, self.other))


@dataclass
class Operator:
    type: str
    name: str
    project_vars: List[Term] = field(init=False)
    triples: Set[Triple]
    order_by: Set[Term] = field(init=False)
    children: Set['Operator']
    expressions: Set[Expression] = field(default_factory=set)
    guid: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __hash__(self):
        return hash((self.guid))


