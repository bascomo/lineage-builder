"""Load mETL Source→Staging lineage into lineage_v2 via gateway."""
import sys, hashlib, hmac, time, json, ssl, urllib.request, functools

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

def gw_select(sql, server='DWH-VDI', db='MetaMart'):
    return gw({'action': 'select_full', 'database': db, 'query': sql, 'server': server})

def gw_exec(sql):
    return gw({'action': 'execute_sql', 'database': 'DWH', 'query': sql, 'server': 'DWHDEDEV-VDI'})

# Step 1: Get Sources first, then mappings per source
print("Step 1: Loading mETL sources...")
sources = gw_select(
    "SELECT SourceId, SourceCode, ServerName, DBName FROM Metadata.Sources WHERE IsDeprecated=0")
print(f"  Found {len(sources)} active sources")

# Load mappings per source
mappings = []
for src in sources:
    try:
        objs = gw_select(
            f"SELECT '{src['SourceCode']}' AS SourceCode, '{src['ServerName']}' AS ServerName, "
            f"'{src['DBName']}' AS DBName, ObjectOriginSchema, ObjectOriginName, ObjectId "
            f"FROM Metadata.Objects WHERE SourceId={src['SourceId']} AND IsDeprecated=0")
        mappings.extend(objs)
        if len(mappings) % 1000 < len(objs):
            print(f"  Loaded {len(mappings)} mappings ({src['SourceCode']})...")
    except Exception as e:
        print(f"  Skip {src['SourceCode']}: {str(e)[:60]}")

print(f"  Total mappings: {len(mappings)}")

# Step 2: Get columns per source
print("\nStep 2: Loading column metadata per source...")
all_fields = []
for src in sources:
    try:
        fields = gw_select(
            f"SELECT f.ObjectId, f.FieldOriginName "
            f"FROM Metadata.ObjectFields f JOIN Metadata.Objects o ON f.ObjectId=o.ObjectId "
            f"WHERE o.SourceId={src['SourceId']} AND f.IsDeprecated=0 AND o.IsDeprecated=0")
        all_fields.extend(fields)
    except:
        pass

print(f"  Total fields: {len(all_fields)}")

# Group fields by ObjectId
fields_by_obj = {}
for f in all_fields:
    oid = f['ObjectId']
    if oid not in fields_by_obj:
        fields_by_obj[oid] = []
    fields_by_obj[oid].append(f['FieldOriginName'])

# Step 3: Build nodes and edges
print("\nStep 3: Building mETL lineage...")
nodes = set()  # (fqn, displayName, typeId, layer)
edges = []     # (srcFqn, tgtFqn, edgeType)

metl_node_fqn = "[mETL]"
nodes.add((metl_node_fqn, "mETL", 33, None))  # mETL Mapping platform

for m in mappings:
    src_server = m['ServerName']
    src_db = m['DBName']
    src_schema = m['ObjectOriginSchema']
    src_table = m['ObjectOriginName']
    src_code = m['SourceCode']
    obj_id = m['ObjectId']

    src_table_fqn = f"{src_server}.{src_db}.{src_schema}.{src_table}"
    stg_table_fqn = f"DWH-VDI.StagingArea.{src_code}.{src_table}"

    nodes.add((src_table_fqn, f"{src_db}.{src_schema}.{src_table}", 5, "Source"))
    nodes.add((stg_table_fqn, f"StagingArea.{src_code}.{src_table}", 5, "Staging"))

    cols = fields_by_obj.get(obj_id, [])
    for col in cols:
        src_col_fqn = f"{src_table_fqn}.{col}"
        stg_col_fqn = f"{stg_table_fqn}.{col}"

        nodes.add((src_col_fqn, col, 6, "Source"))
        nodes.add((stg_col_fqn, col, 6, "Staging"))
        edges.append((src_col_fqn, stg_col_fqn, "DirectCopy"))

print(f"  Nodes: {len(nodes)}")
print(f"  Edges: {len(edges)}")

# Step 4: Write nodes
print(f"\nStep 4: Writing {len(nodes)} mETL nodes...")
written = 0
batch = []
for fqn, display, type_id, layer in nodes:
    f = fqn.replace("'", "''")
    d = display.replace("'", "''")[:450]
    l = f"'{layer}'" if layer else "NULL"
    batch.append(f"({type_id}, N'{f}', N'{d}', {l})")

    if len(batch) >= 50:
        values = ', '.join(batch)
        try:
            gw_exec(
                f"INSERT INTO lineage_v2.Node (NodeTypeId, FullyQualifiedName, DisplayName, LayerName) "
                f"SELECT v.* FROM (VALUES {values}) v(a,b,c,d) "
                f"WHERE NOT EXISTS (SELECT 1 FROM lineage_v2.Node n WHERE n.FullyQualifiedName = v.b)")
            written += len(batch)
            if written % 500 == 0:
                print(f"  Nodes: {written}/{len(nodes)}...")
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

print(f"  Written {written} mETL nodes")

# Step 5: Write edges
print(f"\nStep 5: Writing {len(edges)} DirectCopy edges...")
edge_written = 0
for src, tgt, etype in edges:
    s = src.replace("'", "''")
    t = tgt.replace("'", "''")
    try:
        gw_exec(
            f"INSERT INTO lineage_v2.Edge (SourceNodeId, TargetNodeId, EdgeType) "
            f"SELECT s.NodeId, t.NodeId, '{etype}' "
            f"FROM lineage_v2.Node s CROSS JOIN lineage_v2.Node t "
            f"WHERE s.FullyQualifiedName=N'{s}' AND t.FullyQualifiedName=N'{t}' "
            f"AND NOT EXISTS (SELECT 1 FROM lineage_v2.Edge e WHERE e.SourceNodeId=s.NodeId AND e.TargetNodeId=t.NodeId AND e.EdgeType='{etype}')")
        edge_written += 1
        if edge_written % 500 == 0:
            print(f"  Edges: {edge_written}/{len(edges)}...")
    except:
        pass

print(f"  Written {edge_written} DirectCopy edges")

# Verify
nc = gw({'action': 'select_full', 'database': 'DWH', 'query': 'SELECT COUNT(*) AS Cnt FROM lineage_v2.Node', 'server': 'DWHDEDEV-VDI'})
ec = gw({'action': 'select_full', 'database': 'DWH', 'query': 'SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge', 'server': 'DWHDEDEV-VDI'})
print(f"\n=== lineage_v2: {nc[0]['Cnt']} nodes, {ec[0]['Cnt']} edges ===")
print("Done!")
