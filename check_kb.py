import json
from pathlib import Path
from datetime import datetime

KB = Path("kb")

files = {
    "schema": KB / "kb_schema.json",
    "semantic": KB / "kb_semantic.json",
    "compiled": KB / "compiled_rules.json",
}

def mtime(p: Path):
    return datetime.fromtimestamp(p.stat().st_mtime).isoformat()

def safe_load(p: Path):
    with open(p, "r") as f:
        return json.load(f)

print("\n=== KB FILE COHERENCE CHECK ===")
for k, p in files.items():
    print(f"{k:8} exists={p.exists()} path={p}")

for k, p in files.items():
    if not p.exists():
        raise SystemExit(f"Missing {p}")

schema = safe_load(files["schema"])
semantic = safe_load(files["semantic"])
compiled = safe_load(files["compiled"])

print("\n--- MTIMES ---")
for k, p in files.items():
    print(f"{k:8} mtime={mtime(p)} size={p.stat().st_size} bytes")

print("\n--- CONTENT SNAPSHOT ---")
print("schema.generated_at:", schema.get("generated_at"))
print("schema.tables_count:", len(schema.get("tables", {})))

sem_tables = semantic.get("tables", semantic) if isinstance(semantic, dict) else semantic
print("semantic.tables_count:", len(sem_tables) if hasattr(sem_tables, "__len__") else "unknown")

print("compiled.version:", compiled.get("version"))
print("compiled.schema_name:", compiled.get("schema_name"))
print("compiled.tables_count:", len(compiled.get("tables", {})))

schema_tables = set(schema.get("tables", {}).keys())
compiled_tables = set(compiled.get("tables", {}).keys())

print("\n--- TABLE KEY MISMATCH ---")
print("in_schema_not_in_compiled:", sorted(schema_tables - compiled_tables))
print("in_compiled_not_in_schema:", sorted(compiled_tables - schema_tables))

print("\nOK")
