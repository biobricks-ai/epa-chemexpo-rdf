import gzip,importlib.util,json
from pathlib import Path
R=Path(__file__).parents[1];S=importlib.util.spec_from_file_location("c",R/"stages/convert.py");M=importlib.util.module_from_spec(S);S.loader.exec_module(M);SRC=Path("/mnt/raid2/biobricks/epa-chemexpo/brick")
def test_bounded_streaming_all_modalities(tmp_path):
 r=M.convert(SRC,tmp_path/"x.nt.gz",tmp_path/"r.json",25,25);text=gzip.open(tmp_path/"x.nt.gz","rt").read();assert r["bounded_build"] is True;assert r["composition_rows"]==r["functional_use_rows"]==r["list_presence_rows"]==25;assert r["processed_field_mapping_coverage"]==1;assert "lowerBound" in text or "upperBound" in text;assert "FunctionalUseRecord" in text and "ListPresenceRecord" in text and "product-use-category" in text
def test_full_source_metadata_and_nhex_inventory():
 full={t:__import__('pyarrow.parquet').parquet.ParquetFile(SRC/f"{t}.parquet").metadata.num_rows for t in M.TABLES};assert full["product_composition"]==3574049;assert sum(full.values())==4123009
