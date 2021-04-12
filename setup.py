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

import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="quarry",
    version="0.0.1",
    description="SPARQL queries over OPC UA Information models and time series databases",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/mbapred/quarry",
    author="Magnus Bakken",
    author_email="mba@prediktor.com",
    license="Apache License 2.0",
    classifiers=[
        "License :: OSI Approved :: Apache License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["opcua_tools"],
    include_package_data=True,
    install_requires=[
        "lxml>=4.6.2", "lxml<5.0",
        "pandas>=1.2.2", "pandas<2.0",
        "scipy>=1.6.1", "scipy<2.0",
        "SPARQLWrapper>=1.8.5", "SPARQLWrapper<2.0",
        "rdflib>=5.0.0", "rdflib<6.0",
        "requests>=2.25.1", "requests<3.0",
        "opcua-tools @ https://github.com/PrediktorAS/opcua-tools/tarball/main#egg=opcua-tools-0.0.26"
    ],
    tests_require=[
        "pytest>=6.2.2", "pytest<7.0.0",
        "owlrl>=5.2.1", "owlrl<6.0",
        "psycopg2-binary>=2.8.6", "psycopg2-binary<3.0"
    ]
)