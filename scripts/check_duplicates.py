import os, sys
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from collections import Counter
from services.schema_provider import SchemaProvider

p = SchemaProvider("database_context.json")
p.load()
ids = [t.full_name.lower().strip() for t in p.schema.tables]
dups = [k for k,v in Counter(ids).items() if v > 1]
print("Duplicados:", dups)
