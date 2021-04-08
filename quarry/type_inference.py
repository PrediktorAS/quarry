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
from typing import Set

from rdflib.term import URIRef

from .classes import Operator, Triple, Term, TermConstraint


@dataclass
class Property:
    name: str
    uri: str
    subject_constraints: Set[TermConstraint]
    object_constraints: Set[TermConstraint]


VALUE_VERB = 'http://opcfoundation.org/UA/#value'
STRING_VALUE_VERB = 'http://opcfoundation.org/UA/#stringValue'
REAL_VALUE_VERB = 'http://opcfoundation.org/UA/#realValue'
INT_VALUE_VERB = 'http://opcfoundation.org/UA/#intValue'
BOOL_VALUE_VERB = 'http://opcfoundation.org/UA/#boolValue'
DATATYPE_VALUE_VERBS = {STRING_VALUE_VERB, REAL_VALUE_VERB, INT_VALUE_VERB, BOOL_VALUE_VERB}
TIMESTAMP_VERB = 'http://opcfoundation.org/UA/#timestamp'

PROPERTIES = [
    Property(name='variable_value',
             uri=VALUE_VERB,
             subject_constraints=set(),
             object_constraints={TermConstraint.IS_UA_VARIABLE_VALUE}),
    Property(name='timestamp',
             uri=TIMESTAMP_VERB,
             subject_constraints={TermConstraint.IS_EXTERNAL_UA_VARIABLE_VALUE, TermConstraint.IS_UA_VARIABLE_VALUE},
             object_constraints={TermConstraint.IS_TIMESTAMP}),
    Property(name='string_value',
             uri=STRING_VALUE_VERB,
             subject_constraints={TermConstraint.IS_UA_VARIABLE_VALUE},
             object_constraints={TermConstraint.IS_DATA_VALUE}),
    Property(name='real_value',
             uri=REAL_VALUE_VERB,
             subject_constraints={TermConstraint.IS_UA_VARIABLE_VALUE},
             object_constraints={TermConstraint.IS_DATA_VALUE}),
    Property(name='int_value',
             uri=INT_VALUE_VERB,
             subject_constraints={TermConstraint.IS_UA_VARIABLE_VALUE},
             object_constraints={TermConstraint.IS_DATA_VALUE}),
    Property(name='bool_value',
             uri=BOOL_VALUE_VERB,
             subject_constraints={TermConstraint.IS_UA_VARIABLE_VALUE},
             object_constraints={TermConstraint.IS_DATA_VALUE})
]


def infer_types(op: Operator) -> None:
    infer_types_rec(op=op)


def infer_types_rec(op: Operator):
    for t in op.triples:
        infer_triple_types(triple=t)

    for c in op.children:
        infer_types_rec(op=c)


def infer_triple_types(triple: Triple):
    for p in PROPERTIES:
        if type(triple.verb.rdflib_term) == URIRef and triple.verb.rdflib_term.toPython() == p.uri:
            apply_constraints(triple.subject, p.subject_constraints)
            apply_constraints(triple.object, p.object_constraints)
    if TermConstraint.IS_EXTERNAL_UA_VARIABLE_VALUE in triple.subject.constraints and \
            type(triple.verb.rdflib_term) == URIRef and triple.verb.rdflib_term.toPython() in DATATYPE_VALUE_VERBS:
        triple.object.constraints.add(TermConstraint.IS_EXTERNAL_DATA_VALUE)


def apply_constraints(term: Term, term_constraints: Set[TermConstraint]):
    term.constraints = term.constraints.union(term_constraints)
