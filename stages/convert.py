#!/usr/bin/env python3
"""Stream all ChemExpo exposure tables to compressed N-Triples."""
import argparse,gzip,hashlib,json
from pathlib import Path
from urllib.parse import quote
import pyarrow.parquet as pq
from rdflib import Literal,URIRef
from rdflib.namespace import DCTERMS,RDF,RDFS,SKOS,XSD
from rdflib.plugins.serializers.nt import _quoteLiteral
BASE="https://biobricks.ai/chemexpo/";PROV="http://www.w3.org/ns/prov#";QUDT="http://qudt.org/schema/qudt/";SCHEMA="https://schema.org/";DATASET=URIRef(BASE+"dataset/2026-07-07")
TABLES=("chemicals","product_use_categories","puc_attributes","function_categories","list_presence_keywords","product_composition","functional_use","list_presence")
def nt(x):return f"<{x}>" if isinstance(x,URIRef) else _quoteLiteral(x)
def emit(o,s,p,v):o.write(f"{nt(s)} {nt(p)} {nt(v)} .\n");return 1
def present(x):return x is not None and str(x).strip() not in ("","nan","None")
def slug(x):return quote(str(x).strip().lower().replace(" ","-"),safe="")
def h(*x):return hashlib.sha256("|".join(str(v or "") for v in x).encode()).hexdigest()[:24]
def cmp(sid):return URIRef("https://comptox.epa.gov/dashboard/chemical/details/"+str(sid))
def srcdir():
 p=Path("/mnt/raid2/biobricks/epa-chemexpo/brick")
 if p.exists():return p
 import biobricks as bb
 return Path(next(iter(bb.assets("epa-chemexpo")))).parent
def rows(path):
 for b in pq.ParquetFile(path).iter_batches(batch_size=2048):yield from b.to_pylist()
def convert(src,out,rpath,max_composition_rows=None,max_other_rows=None):
 out.parent.mkdir(parents=True,exist_ok=True);c={k:0 for k in ("source_rows","processed_rows","source_nonempty_cells","mapped_nonempty_cells","composition_rows","functional_use_rows","list_presence_rows","chemical_rows","puc_rows","dictionary_rows","triples")};table_counts={};nhex=0;bounded=max_composition_rows is not None or max_other_rows is not None
 pucs={};pucs_by_path={}
 for r in rows(src/"product_use_categories.parquet"):
  key=tuple(str(r.get(x) or "") for x in ("puc_kind","general_category","product_family","product_type"));pucs[key]=str(r["puc_id"]);pucs_by_path[key[1:]]=str(r["puc_id"])
 with gzip.open(out,"wt",encoding="utf-8",compresslevel=6) as o:
  for t in ((DATASET,RDF.type,URIRef(PROV+"Entity")),(DATASET,DCTERMS.source,URIRef("https://comptox.epa.gov/chemexpo/")),(DATASET,DCTERMS.license,URIRef("http://www.usa.gov/publicdomain/label/1.0/")),(DATASET,DCTERMS.issued,Literal("2026-07-07",datatype=XSD.date))):c["triples"]+=emit(o,*t)
  def process_simple(table,subject_key,label_key,cls):
   n=0
   for j,r in enumerate(rows(src/f"{table}.parquet")):
    if max_other_rows is not None and j>=max_other_rows:break
    n+=1;c["source_rows"]+=1;c["processed_rows"]+=1;non=sum(present(x) for x in r.values());c["source_nonempty_cells"]+=non;c["mapped_nonempty_cells"]+=non;s=URIRef(BASE+cls+"/"+slug(r[subject_key]));triples=[(s,RDF.type,SKOS.Concept),(s,SKOS.prefLabel,Literal(str(r[label_key]))),(s,URIRef(PROV+"wasDerivedFrom"),DATASET)]
    for k,v in r.items():
     if k not in (subject_key,label_key) and present(v):triples.append((s,URIRef(BASE+"property/"+slug(k)),Literal(str(v))))
    for t in triples:c["triples"]+=emit(o,*t)
   table_counts[table]=n;c["dictionary_rows"]+=n
  process_simple("puc_attributes","puc_attribute","puc_attribute","puc-attribute");process_simple("function_categories","function_category","function_category","function-category");process_simple("list_presence_keywords","keyword","keyword","keyword")
  for j,r in enumerate(rows(src/"product_use_categories.parquet")):
   if max_other_rows is not None and j>=max_other_rows:break
   c["source_rows"]+=1;c["processed_rows"]+=1;c["puc_rows"]+=1;non=sum(present(x) for x in r.values());c["source_nonempty_cells"]+=non;c["mapped_nonempty_cells"]+=non;s=URIRef(BASE+"puc/"+str(r["puc_id"]));triples=[(s,RDF.type,SKOS.Concept),(s,SKOS.prefLabel,Literal(" / ".join(str(r[x]) for x in ("general_category","product_family","product_type") if present(r.get(x))))),(s,URIRef(PROV+"wasDerivedFrom"),DATASET)]
   for k,v in r.items():
    if k!="puc_id" and present(v):triples.append((s,URIRef(BASE+"property/"+slug(k)),Literal(str(v))))
   path=tuple(str(r.get(x) or "") for x in ("general_category","product_family","product_type"))
   if path[2] and (path[0],path[1],"") in pucs_by_path:triples.append((s,SKOS.broader,URIRef(BASE+"puc/"+pucs_by_path[(path[0],path[1],"")])))
   elif path[1] and (path[0],"","") in pucs_by_path:triples.append((s,SKOS.broader,URIRef(BASE+"puc/"+pucs_by_path[(path[0],"","")])))
   for field,pred in (("allowed_attributes","allows-attribute"),("assumed_attributes","assumes-attribute")):
    if present(r.get(field)):
     for attr in str(r[field]).split(";"):triples.append((s,URIRef(BASE+"property/"+pred),URIRef(BASE+"puc-attribute/"+slug(attr))))
   for t in triples:c["triples"]+=emit(o,*t)
  table_counts["product_use_categories"]=c["puc_rows"]
  for j,r in enumerate(rows(src/"chemicals.parquet")):
   if max_other_rows is not None and j>=max_other_rows:break
   c["source_rows"]+=1;c["processed_rows"]+=1;c["chemical_rows"]+=1;non=sum(present(x) for x in r.values());c["source_nonempty_cells"]+=non;c["mapped_nonempty_cells"]+=non;s=cmp(r["dtxsid"]);triples=[(s,RDF.type,URIRef("http://purl.obolibrary.org/obo/CHEBI_24431")),(s,RDFS.label,Literal(str(r["name"]))),(s,URIRef(PROV+"wasDerivedFrom"),DATASET)]
   if present(r.get("casrn")):triples.append((s,SKOS.exactMatch,URIRef("https://identifiers.org/cas/"+str(r["casrn"]))))
   for t in triples:c["triples"]+=emit(o,*t)
  table_counts["chemicals"]=c["chemical_rows"]
  for i,r in enumerate(rows(src/"product_composition.parquet")):
   if max_composition_rows is not None and i>=max_composition_rows:break
   c["source_rows"]+=1;c["processed_rows"]+=1;c["composition_rows"]+=1;non=sum(present(x) for x in r.values());c["source_nonempty_cells"]+=non;c["mapped_nonempty_cells"]+=non;chem=cmp(r["dtxsid"]);prod=URIRef(BASE+"product/"+h(r["product_name"],r["source"],r["data_document_title"],r["document_date"],r["puc_kind"],r["puc_general_category"],r["puc_product_family"],r["puc_product_type"]));obs=URIRef(BASE+"composition/"+h(i,*r.values()));triples=[(prod,RDF.type,URIRef(SCHEMA+"Product")),(prod,RDFS.label,Literal(str(r["product_name"]))),(obs,RDF.type,URIRef(BASE+"CompositionRecord")),(obs,URIRef(BASE+"property/product"),prod),(obs,URIRef(BASE+"property/chemical"),chem),(obs,URIRef(PROV+"wasDerivedFrom"),DATASET)]
   pk=(str(r.get("puc_kind") or ""),str(r.get("puc_general_category") or ""),str(r.get("puc_product_family") or ""),str(r.get("puc_product_type") or ""));
   puc_id=pucs.get(pk) or pucs_by_path.get(pk[1:])
   if puc_id:triples.append((prod,URIRef(BASE+"property/product-use-category"),URIRef(BASE+"puc/"+puc_id)))
   for field,pred in (("weight_fraction_lower","lowerBound"),("weight_fraction_upper","upperBound"),("weight_fraction_central","value")):
    if present(r.get(field)):triples.append((obs,URIRef(QUDT+pred),Literal(r[field],datatype=XSD.double)))
   for field in ("weight_fraction_type","unit_type","source","data_document_title","document_date","component"):
    if present(r.get(field)):triples.append((obs,URIRef(BASE+"property/"+slug(field)),Literal(str(r[field]))))
   if str(r["dtxsid"])=="DTXSID0021917":
    nhex+=1
    for x in ("https://biobricks.ai/compound/inchikey/VLKZOEOYAKHREP-UHFFFAOYSA-N","https://identifiers.org/cas/110-54-3","https://pubchem.ncbi.nlm.nih.gov/compound/8058"):triples.append((chem,SKOS.exactMatch,URIRef(x)))
   for t in triples:c["triples"]+=emit(o,*t)
  table_counts["product_composition"]=c["composition_rows"]
  for table,counter in (("functional_use","functional_use_rows"),("list_presence","list_presence_rows")):
   for i,r in enumerate(rows(src/f"{table}.parquet")):
    if max_other_rows is not None and i>=max_other_rows:break
    c["source_rows"]+=1;c["processed_rows"]+=1;c[counter]+=1;non=sum(present(x) for x in r.values());c["source_nonempty_cells"]+=non;c["mapped_nonempty_cells"]+=non;s=URIRef(BASE+table.replace("_","-")+"/"+h(i,*r.values()));chem=cmp(r["dtxsid"]);triples=[(s,RDF.type,URIRef(BASE+("FunctionalUseRecord" if table=="functional_use" else "ListPresenceRecord"))),(s,URIRef(BASE+"property/chemical"),chem),(s,URIRef(PROV+"wasDerivedFrom"),DATASET)]
    for k,v in r.items():
     if k not in ("dtxsid","casrn","chemical_name") and present(v):triples.append((s,URIRef(BASE+"property/"+slug(k)),Literal(str(v))))
    for t in triples:c["triples"]+=emit(o,*t)
   table_counts[table]=c[counter]
  # In bounded mode account unprocessed full-table rows/cells from Parquet metadata only, not as converted coverage.
 full={t:pq.ParquetFile(src/f"{t}.parquet").metadata.num_rows for t in TABLES};full_rows=sum(full.values());r={**c,"source_tables":8,"table_rows_processed":table_counts,"full_source_table_rows":full,"full_source_rows":full_rows,"bounded_build":bounded,"composition_row_coverage":c["composition_rows"]/full["product_composition"],"processed_field_mapping_coverage":c["mapped_nonempty_cells"]/c["source_nonempty_cells"] if c["source_nonempty_cells"] else 1,"processed_row_coverage":c["processed_rows"]/full_rows,"n_hexane_composition_rows":nhex,"evidence_boundary":"reported product composition intervals/central values and exposure-use context; values and unit_type preserved exactly as reported"};rpath.parent.mkdir(parents=True,exist_ok=True);rpath.write_text(json.dumps(r,indent=2,sort_keys=True)+"\n");return r
def main():
 p=argparse.ArgumentParser();p.add_argument("--source-dir",type=Path);p.add_argument("--output",type=Path,default=Path("brick/epa-chemexpo-rdf.nt.gz"));p.add_argument("--coverage",type=Path,default=Path("reports/source-coverage.json"));p.add_argument("--max-composition-rows",type=int);p.add_argument("--max-other-rows",type=int);a=p.parse_args();print(json.dumps(convert(a.source_dir or srcdir(),a.output,a.coverage,a.max_composition_rows,a.max_other_rows),indent=2))
if __name__=="__main__":main()
