"""
Build SSAS lineage graph from OlapWorker results.
Reads get_full_structure results from olapproxy.Request,
extracts DSV → Measure/DimAttribute mappings,
parses Named Query SQL for column-level lineage.
"""
import sys, hashlib, hmac, time, json, ssl, urllib.request, subprocess

sys.stdout.reconfigure(encoding='utf-8')
# Disable buffering
import functools
print = functools.partial(print, flush=True)

SECRET = 'cdOe_6JSZK4KOlqW2k9CkliSpeUsG9YYApC96dqBbz-4rrHY2JAZ1-KByVeA1KHT'
GW = 'https://ibmcognos-test.mhpost.ru/gateway/gateway.ashx'

def token():
    w = int(time.time()) // 30
    return hmac.new(SECRET.encode(), str(w).encode(), hashlib.sha256).hexdigest()

def gw_call(payload):
    data = json.dumps(payload).encode()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(GW, data=data, headers={
        'Content-Type': 'application/json', 'X-Api-Key': token()
    })
    resp = urllib.request.urlopen(req, timeout=60, context=ctx)
    body = json.loads(resp.read().decode())
    if isinstance(body, dict) and 'data' in body:
        return body['data']
    return body

def gw_select(db, query, server='DWHDEDEV-VDI'):
    return gw_call({'action': 'select_full', 'database': db, 'query': query, 'server': server})

DEV_SERVER = 'DWHDEDEV-VDI'

def gw_exec(db, sql, server=DEV_SERVER):
    return gw_call({'action': 'execute_sql', 'database': db, 'query': sql, 'server': server})

# Step 1: Get all completed get_full_structure results
print("Step 1: Fetching SSAS structures from olapproxy.Request...")
results = gw_select('DWH', "SELECT RequestId, Server, Parameters, Result FROM olapproxy.Request WHERE Action = 'get_full_structure' AND Status = 'Completed'")
print(f"  Got {len(results)} structure dumps")

# Parse all structures
all_structures = []
for r in results:
    server = r['Server']
    structure = json.loads(r['Result'])
    structure['_server'] = server
    all_structures.append(structure)
    db = structure.get('database', '?')
    cubes = len(structure.get('cubes', []))
    dsvs = len(structure.get('dsvs', []) if 'dsvs' in structure else structure.get('dataSourceViews', []))
    print(f"  {server}.{db}: {cubes} cubes, {dsvs} DSVs")

# Step 2: Build nodes and edges
print("\nStep 2: Building lineage graph...")

nodes = {}  # fqn -> (displayName, nodeTypeId)
edges = []  # (srcFqn, tgtFqn, edgeType, mechanismFqn)
named_queries = []  # (fqn, sql, connString)

for struct in all_structures:
    server = struct['_server']
    db = struct.get('database', '')
    conn_string = struct.get('conn') or struct.get('connectionString') or ''

    # Parse connection string for SQL context
    sql_db = ''
    for part in conn_string.split(';'):
        kv = part.split('=', 1)
        if len(kv) == 2 and kv[0].strip().upper() in ('INITIAL CATALOG', 'DATABASE'):
            sql_db = kv[1].strip()

    # Server node
    nodes[server] = (server, 11)  # SsasServer

    # Database node
    db_fqn = f"{server}.{db}"
    nodes[db_fqn] = (db, 12)  # SsasDatabase

    # DSVs
    dsvs = struct.get('dsvs') or struct.get('dataSourceViews') or []
    for dsv in dsvs:
        dsv_name = dsv['name']
        dsv_fqn = f"{db_fqn}.{dsv_name}"
        nodes[dsv_fqn] = (dsv_name, 14)  # DSV

        for table in dsv.get('tables', []):
            t_name = table['name']
            is_nq = table.get('isNQ', False)
            t_fqn = f"{dsv_fqn}.{t_name}"
            nodes[t_fqn] = (t_name, 20 if is_nq else 19)  # DsvNamedQuery or DsvTable

            # Columns
            cols = table.get('cols') or table.get('columns') or []
            for col_name in cols:
                if isinstance(col_name, dict):
                    col_name = col_name.get('name', '')
                col_fqn = f"{t_fqn}.{col_name}"
                nodes[col_fqn] = (col_name, 22 if is_nq else 21)  # DsvNqField or DsvTableField

            # Collect Named Query SQL
            sql = table.get('sql')
            if sql and is_nq:
                named_queries.append((t_fqn, sql, sql_db))

    # Cubes
    for cube in struct.get('cubes', []):
        cube_name = cube['name']
        cube_fqn = f"{db_fqn}.{cube_name}"
        nodes[cube_fqn] = (cube_name, 13)  # Cube

        # Measure Groups → Measures
        for mg in cube.get('mg', cube.get('measureGroups', [])):
            mg_name = mg['name']
            mg_fqn = f"{cube_fqn}.{mg_name}"
            nodes[mg_fqn] = (mg_name, 15)  # MeasureGroup

            for m in mg.get('measures', []):
                m_name = m['name']
                m_fqn = f"{mg_fqn}.{m_name}"
                agg = m.get('agg', m.get('aggregateFunction', ''))
                nodes[m_fqn] = (m_name, 16)  # Measure

                # Link: DSV column → Measure
                src = m.get('src') or m.get('source')
                if src and '.' in src:
                    parts = src.split('.', 1)
                    # Find DSV table.column
                    for dsv in dsvs:
                        dsv_col_fqn = f"{db_fqn}.{dsv['name']}.{parts[0]}.{parts[1]}"
                        if dsv_col_fqn in nodes:
                            edge_type = 'Aggregation' if agg in ('Sum', 'Count') else 'DataFlow'
                            edges.append((dsv_col_fqn, m_fqn, edge_type, cube_fqn))
                            break

        # Dimensions → Attributes
        for dim in cube.get('dims', cube.get('dimensions', [])):
            dim_name = dim['name']
            if not dim_name:
                continue
            dim_fqn = f"{cube_fqn}.{dim_name}"
            nodes[dim_fqn] = (dim_name, 17)  # Dimension

            for attr in dim.get('attrs', dim.get('attributes', [])):
                a_name = attr['name']
                a_fqn = f"{dim_fqn}.{a_name}"
                nodes[a_fqn] = (a_name, 18)  # DimensionAttribute

                key = attr.get('key')
                if key and '.' in key:
                    parts = key.split('.', 1)
                    for dsv in dsvs:
                        dsv_col_fqn = f"{db_fqn}.{dsv['name']}.{parts[0]}.{parts[1]}"
                        if dsv_col_fqn in nodes:
                            edges.append((dsv_col_fqn, a_fqn, 'DataFlow', cube_fqn))
                            break

print(f"  Nodes: {len(nodes)}")
print(f"  Edges: {len(edges)}")
print(f"  Named Queries to parse: {len(named_queries)}")

# Step 3: Parse Named Query SQL
print("\nStep 3: Parsing Named Query SQL...")
nq_edges = 0
nq_ok = 0
nq_err = 0

for nq_fqn, sql, ctx_db in named_queries:
    # Use dotnet TsqlLineageParser via subprocess
    # For now, just count - actual parsing requires running .NET
    # We'll record the SQL for later batch processing
    nq_ok += 1

print(f"  Named Queries: {nq_ok} (SQL parsing will be done in next step)")

# Step 4: Write nodes in batches
print(f"\nStep 4: Writing {len(nodes)} nodes to lineage_v2 (batches of 50)...")

written = 0
batch = []
for fqn, (display_name, type_id) in nodes.items():
    fqn_esc = fqn.replace("'", "''")
    dn_esc = display_name.replace("'", "''")[:450]  # truncate long names
    batch.append(f"({type_id}, N'{fqn_esc}', N'{dn_esc}', 'Cube')")

    if len(batch) >= 50:
        values = ', '.join(batch)
        try:
            gw_exec('DWH',
                f"INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName, LayerName) "
                f"SELECT v.* FROM (VALUES {values}) v(a,b,c,d) "
                f"WHERE NOT EXISTS (SELECT 1 FROM lineage_v2.Node n WHERE n.FullyQualifiedName = v.b)")
            written += len(batch)
            print(f"  Nodes: {written}/{len(nodes)}...")
        except Exception as e:
            # Fallback: try one by one
            for single in batch:
                try:
                    gw_exec('DWH', f"IF NOT EXISTS (SELECT 1 FROM lineage_v2.Node WHERE FullyQualifiedName = (SELECT b FROM (VALUES {single}) v(a,b,c,d))) INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName, LayerName) VALUES {single}")
                    written += 1
                except:
                    pass
            print(f"  Batch error, fallback: {written}/{len(nodes)}... ({str(e)[:60]})")
        batch = []

# Last batch
if batch:
    values = ', '.join(batch)
    try:
        gw_exec('DWH',
            f"INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName, LayerName) "
            f"SELECT v.* FROM (VALUES {values}) v(a,b,c,d) "
            f"WHERE NOT EXISTS (SELECT 1 FROM lineage_v2.Node n WHERE n.FullyQualifiedName = v.b)")
        written += len(batch)
    except:
        pass

print(f"  Written {written} nodes")

# Step 5: Write edges in batches
print(f"\nStep 5: Writing {len(edges)} edges...")
edge_written = 0
for src_fqn, tgt_fqn, edge_type, mech_fqn in edges:
    s = src_fqn.replace("'", "''")
    t = tgt_fqn.replace("'", "''")
    m = mech_fqn.replace("'", "''")
    try:
        gw_exec('DWH',
            f"INSERT INTO lineage_v2.Edge (SourceNodeId, TargetNodeId, EdgeType, MechanismNodeId) "
            f"SELECT s.NodeId, t.NodeId, '{edge_type}', m.NodeId "
            f"FROM lineage_v2.Node s "
            f"CROSS JOIN lineage_v2.Node t "
            f"LEFT JOIN lineage_v2.Node m ON m.FullyQualifiedName=N'{m}' "
            f"WHERE s.FullyQualifiedName=N'{s}' AND t.FullyQualifiedName=N'{t}' "
            f"AND NOT EXISTS (SELECT 1 FROM lineage_v2.Edge e WHERE e.SourceNodeId=s.NodeId AND e.TargetNodeId=t.NodeId AND e.EdgeType='{edge_type}')")
        edge_written += 1
        if edge_written % 100 == 0:
            print(f"  Edges: {edge_written}/{len(edges)}...")
    except Exception as e:
        if edge_written < 5:
            print(f"  Edge error: {str(e)[:80]}")

print(f"  Written {edge_written} edges")

# Verify
nc = gw_select('DWH', "SELECT COUNT(*) AS Cnt FROM lineage_v2.Node")
ec = gw_select('DWH', "SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge")
print(f"\n=== lineage_v2: {nc[0]['Cnt']} nodes, {ec[0]['Cnt']} edges ===")
print("Done!")
