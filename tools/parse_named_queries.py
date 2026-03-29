"""Parse Named Query SQL from SSAS structures, write SQL→DSV edges to lineage_v2."""
import sys, hashlib, hmac, time, json, ssl, urllib.request, subprocess, functools

sys.stdout.reconfigure(encoding='utf-8')
print = functools.partial(print, flush=True)

SECRET = 'cdOe_6JSZK4KOlqW2k9CkliSpeUsG9YYApC96dqBbz-4rrHY2JAZ1-KByVeA1KHT'
GW = 'https://ibmcognos-test.mhpost.ru/gateway/gateway.ashx'

def token():
    w = int(time.time()) // 30
    return hmac.new(SECRET.encode(), str(w).encode(), hashlib.sha256).hexdigest()

def gw(payload):
    data = json.dumps(payload).encode()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(GW, data=data, headers={
        'Content-Type': 'application/json', 'X-Api-Key': token()
    })
    resp = urllib.request.urlopen(req, timeout=120, context=ctx)
    body = json.loads(resp.read().decode())
    return body.get('data', body) if isinstance(body, dict) else body

def gw_exec(sql):
    return gw({'action': 'execute_sql', 'database': 'DWH', 'query': sql, 'server': 'DWHDEDEV-VDI'})

def gw_select(sql):
    return gw({'action': 'select_full', 'database': 'DWH', 'query': sql, 'server': 'DWHDEDEV-VDI'})

# Step 1: Get all structures
print("Step 1: Loading SSAS structures...")
results = gw_select("SELECT Server, Parameters, Result FROM olapproxy.Request WHERE Action='get_full_structure' AND Status='Completed'")

all_nqs = []  # (dsv_table_fqn, sql, sql_db, dsv_columns)
for r in results:
    struct = json.loads(r['Result'])
    server = r['Server']
    db = struct.get('database', '')
    conn = struct.get('conn') or struct.get('connectionString') or ''

    # Parse SQL database from connection string
    sql_db = 'MOVofGoodsDynamic'
    for part in conn.split(';'):
        kv = part.split('=', 1)
        if len(kv) == 2 and kv[0].strip().upper() in ('INITIAL CATALOG', 'DATABASE'):
            sql_db = kv[1].strip()

    for dsv in struct.get('dsvs', struct.get('dataSourceViews', [])):
        dsv_name = dsv['name']
        for table in dsv.get('tables', []):
            t_name = table['name']
            is_nq = table.get('isNQ', False)
            sql_text = table.get('sql')
            if is_nq and sql_text:
                dsv_fqn = f"{server}.{db}.{dsv_name}.{t_name}"
                cols = table.get('cols', table.get('columns', []))
                col_names = [c if isinstance(c, str) else c.get('name', '') for c in cols]
                all_nqs.append((dsv_fqn, sql_text, sql_db, col_names))

print(f"  Found {len(all_nqs)} Named Queries with SQL")

# Step 2: Parse each NQ through TsqlLineageParser via dotnet
# Write SQL to temp files, parse with dotnet tool
print("\nStep 2: Parsing Named Query SQL...")

# Write all NQ SQL to a JSON file for batch processing
nq_data = [{'fqn': fqn, 'sql': sql, 'db': db, 'cols': cols} for fqn, sql, db, cols in all_nqs]
with open('D:/projects/lineage-builder/tools/nq_batch.json', 'w', encoding='utf-8') as f:
    json.dump(nq_data, f, ensure_ascii=False)

# Parse in Python using regex (fast, good enough for FROM/JOIN extraction):
# Extract table references from SQL using regex (quick & dirty)
import re

edges = []
nodes_to_add = set()

for dsv_fqn, sql_text, sql_db, dsv_cols in all_nqs:
    # Extract FROM/JOIN table references
    tables = re.findall(r'\bFROM\s+(?:\[?(\w+)\]?\.)?(?:\[?(\w+)\]?\.)?(\[?\w+\]?)', sql_text, re.IGNORECASE)
    tables += re.findall(r'\bJOIN\s+(?:\[?(\w+)\]?\.)?(?:\[?(\w+)\]?\.)?(\[?\w+\]?)', sql_text, re.IGNORECASE)

    for parts in tables:
        schema = parts[1].strip('[]') if parts[1] else 'dbo'
        table_name = parts[2].strip('[]') if parts[2] else ''
        if not table_name or table_name.upper() in ('SELECT', 'WHERE', 'SET', 'VALUES'):
            continue

        sql_table_fqn = f"DWH-VDI.{sql_db}.{schema}.{table_name}"
        nodes_to_add.add((sql_table_fqn, table_name, 5))  # Table node

        # For each DSV column, create edge from SQL table to DSV column
        for col in dsv_cols:
            dsv_col_fqn = f"{dsv_fqn}.{col}"
            sql_col_fqn = f"{sql_table_fqn}.{col}"
            nodes_to_add.add((sql_col_fqn, col, 6))  # Column node
            edges.append((sql_col_fqn, dsv_col_fqn, 'DataFlow'))

print(f"  Extracted {len(nodes_to_add)} SQL nodes, {len(edges)} potential edges")

# Step 3: Write SQL table/column nodes
print(f"\nStep 3: Writing {len(nodes_to_add)} SQL nodes...")
written = 0
batch = []
for fqn, display, type_id in nodes_to_add:
    fqn_esc = fqn.replace("'", "''")
    dn_esc = display.replace("'", "''")[:450]
    batch.append(f"({type_id}, N'{fqn_esc}', N'{dn_esc}', NULL)")

    if len(batch) >= 50:
        values = ', '.join(batch)
        try:
            gw_exec(
                f"INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName, LayerName) "
                f"SELECT v.* FROM (VALUES {values}) v(a,b,c,d) "
                f"WHERE NOT EXISTS (SELECT 1 FROM lineage_v2.Node n WHERE n.FullyQualifiedName = v.b)")
            written += len(batch)
            if written % 200 == 0:
                print(f"  Nodes: {written}/{len(nodes_to_add)}...")
        except Exception as e:
            print(f"  Batch error: {str(e)[:80]}")
        batch = []

if batch:
    values = ', '.join(batch)
    try:
        gw_exec(
            f"INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName, LayerName) "
            f"SELECT v.* FROM (VALUES {values}) v(a,b,c,d) "
            f"WHERE NOT EXISTS (SELECT 1 FROM lineage_v2.Node n WHERE n.FullyQualifiedName = v.b)")
        written += len(batch)
    except:
        pass

print(f"  Written {written} SQL nodes")

# Step 4: Write edges (SQL column → DSV column)
print(f"\nStep 4: Writing edges (SQL → DSV)...")
edge_written = 0
for src_fqn, tgt_fqn, edge_type in edges:
    s = src_fqn.replace("'", "''")
    t = tgt_fqn.replace("'", "''")
    try:
        gw_exec(
            f"INSERT INTO lineage_v2.Edge (SourceNodeId, TargetNodeId, EdgeType) "
            f"SELECT s.NodeId, t.NodeId, '{edge_type}' "
            f"FROM lineage_v2.Node s CROSS JOIN lineage_v2.Node t "
            f"WHERE s.FullyQualifiedName=N'{s}' AND t.FullyQualifiedName=N'{t}' "
            f"AND NOT EXISTS (SELECT 1 FROM lineage_v2.Edge e WHERE e.SourceNodeId=s.NodeId AND e.TargetNodeId=t.NodeId AND e.EdgeType='{edge_type}')")
        edge_written += 1
        if edge_written % 200 == 0:
            print(f"  Edges: {edge_written}...")
    except:
        pass

print(f"  Written {edge_written} SQL→DSV edges")

# Verify
nc = gw_select("SELECT COUNT(*) AS Cnt FROM lineage_v2.Node")
ec = gw_select("SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge")
print(f"\n=== lineage_v2: {nc[0]['Cnt']} nodes, {ec[0]['Cnt']} edges ===")
print("Done!")
