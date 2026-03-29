"""Parse views from MOVofGoodsDynamic, write View→Table column edges to lineage_v2."""
import sys, hashlib, hmac, time, json, ssl, urllib.request, re, functools

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

def gw_select(sql, server='DWH-VDI', db='MetaMart'):
    return gw({'action': 'select_full', 'database': db, 'query': sql, 'server': server})

# Step 1: Fetch views from MOVofGoodsDynamic (key database for cubes)
print("Step 1: Fetching views from MOVofGoodsDynamic...")
# Only views with reasonable size, most relevant for lineage
views = gw_select(
    "SELECT SchemaName, ObjectName, ObjectDefinition "
    "FROM lineage.AllObjectsDefinition "
    "WHERE ObjectType='V' AND DBName='MOVofGoodsDynamic' AND ServerName='DWH-VDI' "
    "AND ObjectDefinition IS NOT NULL AND LEN(ObjectDefinition) BETWEEN 50 AND 8000")
print(f"  Got {len(views)} views")

# Step 2: Parse each view with regex (extract FROM/JOIN tables and SELECT columns)
print("\nStep 2: Parsing views...")
SERVER = 'DWH-VDI'
DB = 'MOVofGoodsDynamic'

nodes = set()
edges = []

for v in views:
    schema = v['SchemaName']
    name = v['ObjectName']
    sql = v['ObjectDefinition']
    view_fqn = f"{SERVER}.{DB}.{schema}.{name}"

    # Add view node
    nodes.add((view_fqn, f"{schema}.{name}", 7, "Core"))  # View type

    # Extract referenced tables from FROM/JOIN
    table_refs = re.findall(
        r'(?:FROM|JOIN)\s+(?:\[?(\w+)\]?\.)?(?:\[?(\w+)\]?\.)?(\[?\w+\]?)',
        sql, re.IGNORECASE)

    source_tables = set()
    for parts in table_refs:
        t_schema = parts[1].strip('[]') if parts[1] else 'dbo'
        t_name = parts[2].strip('[]') if parts[2] else ''
        if not t_name or t_name.upper() in ('SELECT', 'WHERE', 'SET', 'VALUES', 'NULL', 'CASE', 'WHEN'):
            continue
        t_fqn = f"{SERVER}.{DB}.{t_schema}.{t_name}"
        source_tables.add((t_fqn, t_name, t_schema))
        nodes.add((t_fqn, f"{t_schema}.{t_name}", 5, "Core"))  # Table

    # Extract SELECT column aliases (output columns of the view)
    # Pattern: "AS alias" or "column_name" at end of select element
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_list = select_match.group(1)
        # Extract aliases after AS or standalone column names
        aliases = re.findall(r'\bAS\s+\[?(\w+)\]?', select_list, re.IGNORECASE)

        for alias in aliases:
            view_col_fqn = f"{view_fqn}.{alias}"
            nodes.add((view_col_fqn, alias, 6, "Core"))

            # Create edge from each source table's same-named column to view column
            for t_fqn, t_name, t_schema in source_tables:
                src_col_fqn = f"{t_fqn}.{alias}"
                nodes.add((src_col_fqn, alias, 6, "Core"))
                edges.append((src_col_fqn, view_col_fqn, "DataFlow"))

# Deduplicate edges
edges = list(set(edges))
print(f"  Nodes: {len(nodes)}, Edges: {len(edges)}")

# Step 3: Write nodes
print(f"\nStep 3: Writing {len(nodes)} nodes...")
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
            print(f"  Error: {str(e)[:80]}")
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

print(f"  Written {written} nodes")

# Step 4: Write edges
print(f"\nStep 4: Writing {len(edges)} View edges...")
ew = 0
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
        ew += 1
        if ew % 500 == 0:
            print(f"  Edges: {ew}/{len(edges)}...")
    except:
        pass

print(f"  Written {ew} edges")

nc = gw({'action': 'select_full', 'database': 'DWH', 'query': 'SELECT COUNT(*) AS Cnt FROM lineage_v2.Node', 'server': 'DWHDEDEV-VDI'})
ec = gw({'action': 'select_full', 'database': 'DWH', 'query': 'SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge', 'server': 'DWHDEDEV-VDI'})
print(f"\n=== lineage_v2: {nc[0]['Cnt']} nodes, {ec[0]['Cnt']} edges ===")
print("Done!")
