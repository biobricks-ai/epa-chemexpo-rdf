import json
from pathlib import Path
R=Path(__file__).parents[1]
def test_health():
 assert (R/".bb/dependencies.txt").read_text().strip()=="epa-chemexpo";b=json.loads((R/"health/build-status.json").read_text());assert b["status"]=="validated-bounded-build" and b["full_source_build"] is False;assert json.loads((R/"health/source-audit.json").read_text())["license"]=="us-public-domain";assert json.loads((R/"health/ontology-policy.json").read_text())["score"]>=80
