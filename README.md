# Quarry
This repository provides prototype Python tools for: 
- translating OPC UA NodeSet2 XML files to semantic web technologies (SWT) 
- jointly querying OPC UA information models in a SPARQL database and data in a time series database

This allows the addition of information models to existing time series database / data lake / historian infrastructure.\
The code is part of a (hopefully!) forthcoming paper. 
## Overview
### Translation
First, OPC UA NodeSet2 xml files corresponding to OPC UA Information Models are parsed. We translate to the Semantic Web corresponding to the translation described in the associated paper. 
Turtle-files (.ttl) are produced, and these may be loaded into an arbitrary SPARQL database. For queries over models and time series data to to work, it is assumed that a time series database exists. 
We will use the term Signal ID to refer to data identifiers in this time series database. We assume that these Signal IDs can be linked to OPC UA Variable Nodes from the OPC UA Information Model in the Nodeset2 XML-files, and expect a comma separated file to contain this information.
### Queries
![](images/sequence.png)\
Queries are preprocessed using [RDFLib](https://github.com/RDFLib/rdflib) in order to: 
- removes references to data in the time series database, such as variables and filters they occur in
- add query variables resolving the Signal ID corresponding to a UA Variable

We use [SPARQLWrapper](https://github.com/RDFLib/sparqlwrapper) and wrap the SPARQL endpoint. 
The modified SPARQL query is sent to this endpoint. Based on the response from this endpoint and the original query, one or more queries targeting a time series database is produced.
We combine the results of the SPARQL query and the responses from the time series database using [Pandas](https://github.com/pandas-dev/pandas) to produce the full result.
## Example
Example goes here, soon...\
Look at tests/test_split_query.py for an example meanwhile. 

## Usage
Under construction..
## Time series database support
In the tests, a PostgreSQL docker image is used to store time series data.
Support can however be added for other time series databases in the following way. 

... TBD

## Known issues
- We currently do not implement a SPARQL endpoint as this is outside of the scope of the prototype. 
- The result combination approach is currently somewhat ad hoc, as we rely on suffixes of column names in order to combine the result correctly.
- Signal IDs are currently natural numbers only.  
## License
The code in this repository is copyrighted to [Prediktor AS](http://prediktor.com), and is licensed under the Apache 2.0. \
Exceptions apply to some of the test data (see document headers for license information). 

Author:
[Magnus Bakken](mba@prediktor.com)

