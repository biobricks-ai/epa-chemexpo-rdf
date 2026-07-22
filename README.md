# epa-chemexpo-rdf

Streaming RDF integration for EPA ChemExpo/CPDat exposure evidence. ChemExpo is U.S. EPA public-domain data, with row-level third-party provenance retained.

The transform covers all eight source tables: 3,574,049 product-composition rows, chemicals, PUC hierarchy and attributes, functional-use records and categories, list-presence records and keyword definitions. A product IRI hashes product name together with source, document, date and PUC path because the bulk export supplies no durable product identifier. Composition records preserve lower, upper and central weight-fraction values separately, along with the reported `weight_fraction_type` and `unit_type`; values are not silently rescaled.

The committed DVC stage is a bounded 10,000-row-per-large-table validation build. Run `python stages/convert.py` for the full streaming build. The full source contains 4,123,009 rows and is expected to produce tens of millions of triples and a multi-gigabyte compressed artifact, so completion is not claimed until that command finishes and the health report is replaced.

Chemicals use CompTox DTXSID IRIs and valid CAS links. The full composition table contains 3,235 n-hexane rows; every emitted n-hexane row adds InChIKey, CAS, PubChem and DTXSID reachability.
