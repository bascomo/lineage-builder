"""Load SQL Agent Jobs→Steps→SSIS/SP relationships into lineage_v2."""
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

# Step 1: Load jobs
print("Step 1: Loading SQL Agent Jobs...")
jobs = gw_select("SELECT job_id, name, enabled, description FROM lineage.Jobs")
print(f"  Found {len(jobs)} jobs")

# Step 2: Load steps
print("Step 2: Loading Job Steps...")
steps = gw_select("SELECT job_id, step_id, step_name, subsystem, command, database_name FROM lineage.JobSteps")
print(f"  Found {len(steps)} steps")

# Build nodes and edges
nodes = set()
edges = []

for job in jobs:
    job_name = job['name']
    job_fqn = f"[SqlAgent].DWH-VDI.{job_name}"
    nodes.add((job_fqn, job_name, 30, None))

    # Find steps for this job
    job_steps = [s for s in steps if s['job_id'] == job['job_id']]
    for step in job_steps:
        step_name = step['step_name']
        step_id = step['step_id']
        subsystem = step.get('subsystem', '')
        command = step.get('command', '') or ''
        database = step.get('database_name', '')

        step_fqn = f"{job_fqn}.Step{step_id}"
        nodes.add((step_fqn, f"{job_name} > {step_name}", 31, None))
        edges.append((job_fqn, step_fqn, "ProcessExecution"))

        # Link to SSIS packages
        if 'SSIS' in subsystem.upper():
            idx = command.find('.dtsx')
            if idx > 0:
                start = max(command.rfind('\\', 0, idx), command.rfind('/', 0, idx), 0)
                if start > 0: start += 1
                pkg_name = command[start:idx]
                pkg_fqn = f"[SSIS].{pkg_name}"
                nodes.add((pkg_fqn, pkg_name, 25, None))
                edges.append((step_fqn, pkg_fqn, "ProcessExecution"))

        # Link to stored procedures
        if 'TSQL' in subsystem.upper() and command:
            for line in command.split('\n'):
                line = line.strip()
                if line.upper().startswith('EXEC'):
                    parts = line.split(None, 2)
                    if len(parts) >= 2:
                        proc_name = parts[1].strip('[]\'"')
                        if not proc_name.startswith('@') and not proc_name.startswith('sp_'):
                            proc_fqn = f"DWH-VDI.{database}.dbo.{proc_name}" if database else f"DWH-VDI.master.dbo.{proc_name}"
                            nodes.add((proc_fqn, proc_name, 8, None))
                            edges.append((step_fqn, proc_fqn, "ProcessExecution"))

edges = list(set(edges))
print(f"\nNodes: {len(nodes)}, Edges: {len(edges)}")

# Write nodes
print(f"\nStep 3: Writing {len(nodes)} job nodes...")
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
        except:
            pass
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

# Write edges
print(f"\nStep 4: Writing {len(edges)} edges...")
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
    except:
        pass
print(f"  Written {ew} edges")

nc = gw({'action': 'select_full', 'database': 'DWH', 'query': 'SELECT COUNT(*) AS Cnt FROM lineage_v2.Node', 'server': 'DWHDEDEV-VDI'})
ec = gw({'action': 'select_full', 'database': 'DWH', 'query': 'SELECT COUNT(*) AS Cnt FROM lineage_v2.Edge', 'server': 'DWHDEDEV-VDI'})
print(f"\n=== lineage_v2: {nc[0]['Cnt']} nodes, {ec[0]['Cnt']} edges ===")
print("Done!")
