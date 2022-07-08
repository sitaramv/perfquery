#/usr/bin/python3

import sys
import os
import math
import uuid
import json
import time
import random
import requests
import urllib3
import decimal
import datetime
import multiprocessing


cfg = {
       "host": 'http://ec2-44-235-91-186.us-west-2.compute.amazonaws.com',         # querynode host ip
       "workload" : "complex",                # workload type (see workloads)
       "load":"90",                          # load percent (see loads)
       "loaddata": True,                      # control bucket/scope/collection/index/data drop/creation/load
       "execute": False,                      # control execute queries
       "nthreads" : 85,                       # max number of client threads (might lowered by load setting)
       "datareplicas": 2,                     # data replica setting
       "indexreplicas": 1,                    # data ndex replicas setting
       "memory": 24576,                       # datanode memory  (divided by nbuckets)
       "nbuckets": 20,                        # number of bucktes
       "nscopes"   : 2,                       # number of scopes per bucket
       "dataweightdocs" : 1000000,            # number of docs per each wieght
       "dataweightpercollection" : 5,         # number of wieght per collection
       "nindexes": 1,                         # number of indexes per collection
       "naindexes": 1,                        # number of array indexes per collection
       "batchsize": 100,                      # batchsize (i.e. qualified rows per query)
       "qualifiedbatches": 1,                 # number of batches (to increase qualified rows per query)
       "duration": 600,                       # execution duration in seconds
       "ncores": 16,                          # number of query service cores
       "indexfile": "index.txt",              # index statements
       "workloadfile": "workload",            # workload statements
       "backupdir":"/data/backups",           # backup dir
       "usebackup":True,                      # use backup/restore vs load_data
       "indexes"  : [ "CREATE INDEX ix0 IF NOT EXISTS ON col00 (c0, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ix1 IF NOT EXISTS ON col00 (c1, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ix2 IF NOT EXISTS ON col00 (c2, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ix3 IF NOT EXISTS ON col00 (c3, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ix4 IF NOT EXISTS ON col00 (c4, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }"
                    ],
       "aindexes"  : ["CREATE INDEX ixa0 IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac0) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ixa1 IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac1) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ixa2 IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac2) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ixa3 IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac3) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ixa4 IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac4) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }"
                    ],
       "aqueries" : {"q0": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d USE INDEX(`#sequential`) WHERE d.c0 BETWEEN $start AND $end",
                     "q1": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end",
                     "q2": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end ORDER BY d.c0 DESC LIMIT $limit",
                     "q3": "SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col00 AS l JOIN col00 AS r USE HASH(BUILD) ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end",
                     "q4": "SELECT g1, COUNT(1) AS cnt FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end GROUP BY IMOD(d.id,10) AS g1 ORDER BY g1",
                     "q5": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE ANY v IN d.a1 SATISFIES v.ac0 BETWEEN $start AND $end AND v.aid = 2 END",
                     "q6": "WITH cte AS (SELECT RAW t FROM col00 AS t WHERE t.c0 BETWEEN $start AND $end) SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col00 AS l JOIN cte AS r ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end",
                     "q7": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d UNNEST d.a1 AS u WHERE u.ac0 BETWEEN $start AND $end AND u.aid = 1",
                     "q8": "UPDATE col00 AS d SET d.comment = d.comment WHERE d.c0 BETWEEN $start AND $end",
                     "q9": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end AND udf(d.c0) = d.c0"
                    },
      "workloads":{"q0": {"q0":20}, "q1": {"q1":20}, "q2": {"q2":20}, "q3": {"q3":20}, "q4": {"q4":20},
                   "q5": {"q5":20}, "q6": {"q6":20}, "q7": {"q7":20}, "q8": {"q8":20}, "q9": {"q9":20},
                   "simple":{"q0":0, "q1":10, "q2":3, "q3":3, "q4":2, "q5":2, "q6":0, "q7":0, "q8":0, "q9":0},
                   "medium":{"q0":0, "q1":6, "q2":4, "q3":3, "q4":2, "q5":2, "q6":2, "q7":1, "q8":0, "q9":0},
                   "complex":{"q0":0, "q1":5, "q2":3, "q3":2, "q4":2, "q5":2, "q6":2, "q7":2, "q8":1, "q9":1}},
      "loads": { 
              "dataweight":        {"free":1, "light":10, "moderate": 30, "overheavy": 60, "superheavy": 120},
#             "querytenantweight": {"free":1, "light":2, "moderate": 6, "overheavy": 12, "superheavy": 24}, # default no of collections 1, 2, 6, 12, 24
              "50":{"free": 5, "light":8, "moderate":5, "overheavy":1, "superheavy":1 },
              "90":{"free": 0, "light":5, "moderate":10, "overheavy":4, "superheavy":1 },
              "100": {"free": 1, "light":1, "moderate":2, "overheavy":0, "superheavy":0 }
               }
      }

def workload_init():
       load = cfg["loads"][cfg["load"]]
       ntenants = 0
       for k in sorted(load.keys()) :
           ntenants = ntenants + load[k]
       factor = int(cfg["nbuckets"]/ntenants)
       ad = []
       ototal = 0
       for k in sorted(load.keys()) :
           if load[k] == 0 :
               continue
           batches = int((cfg["loads"]["dataweight"][k] * cfg["dataweightdocs"])/cfg["batchsize"])
           for nc in range(0, factor * load[k]) :
                ad.append({"type":k, "batches": batches})
                if k == "overheavy" :
                   ototal = ototal + 1
                elif k == "superheavy" :
                   ototal = ototal + 4
       batchespercollection = (cfg["dataweightpercollection"]*cfg["dataweightdocs"])/cfg["batchsize"]
       workload = {}
       memory = int(cfg["memory"] - 1024*cfg["nbuckets"])
       if memory < 0 :
           memory = 0
       for bv in range(0, cfg["nbuckets"]) :
          bc = "b" + str(bv).zfill(2)
          workload[bc] = ad[bv].copy()
          bmemory = 1024
          if ad[bv]["type"] == "overheavy" :
              bmemory = bmemory + int(memory/ototal)
          elif ad[bv]["type"] == "superheavy" :
              bmemory = bmemory + 4*int(memory/ototal)
          workload[bc]["memory"] = bmemory
          scopes = []
          ncollections = int(ad[bv]["batches"]/batchespercollection)
          nscopes = cfg["nscopes"]
          if ncollections == 0 :
              nscopes = 1 
              ncollections = 1
          else :
              ncollections = int(ncollections/nscopes)
          workload[bc]["ncollections"] = ncollections
          for sv in range(0,nscopes) :
             sc = "s" + str(sv).zfill(2)
             qc = bc + "." + sc
             collections = []
             for cv in range(0,ncollections) :
                collection = "col" + str(cv).zfill(2)
                bindexes = ""

                ddls = []
                for iv in range(0,cfg["nindexes"]) :
                   if bindexes != "" :
                      bindexes = bindexes + ", "
                   bindexes = bindexes + "ix" + str(iv)

                   stmt = cfg["indexes"][iv].replace("col00", collection).replace("indexreplicas",str(cfg["indexreplicas"]))
                   ddls.append(stmt)

                for iv in range(0,cfg["naindexes"]) :
                   if bindexes != "" :
                      bindexes = bindexes + "," 
                   bindexes = bindexes + "ixa" + str(iv)

                   stmt = cfg["aindexes"][iv].replace("col00", collection).replace("indexreplicas",str(cfg["indexreplicas"]))
                   ddls.append(stmt)
                ddls.append("BUILD INDEX ON " + collection + " (" + bindexes + ")")
                batches = int(ad[bv]["batches"]/(ncollections*nscopes))
                collections.append({"sc":qc, "name":collection, "ddls": ddls, "batchsize": cfg["batchsize"], "batches": batches})
             scopes.append({"name": sc, "bc": bc, "collections": collections})
          workload[bc]["batchsize"] = cfg["batchsize"]
          workload[bc]["scopes"] = scopes
       return workload

def systemcmd(cmd) :
    print (cmd)
    os.system(cmd)

# bucket/scope/collection re-creation

def create_collections(workload):
    if not cfg["loaddata"] :
       return

    host = cfg["host"]
    replicas = cfg["datareplicas"]
    
    systemcmd("curl -s -u Administrator:password " + host + ":8091/internalSettings -d 'maxBucketCount=80'")
    systemcmd("curl -s -u Administrator:password " + host + ":8091/settings/querySettings -d 'queryCompletedLimit=0'")
    systemcmd("curl -s -u Administrator:password " + host + ":8091/settings/querySettings -d 'queryPreparedLimit=100000'")
    for b in sorted(workload.keys()):
        systemcmd("/opt/couchbase/bin/couchbase-cli bucket-delete -c " + host + " -u Administrator -p password --bucket " + b )

    for b in sorted(workload.keys()):
        bv = workload[b]
        cmd = "/opt/couchbase/bin/couchbase-cli bucket-create -c " + host + " -u Administrator -p password --bucket "
        cmd += b
        cmd += " --bucket-ramsize " + str(bv["memory"])
        cmd += " --bucket-replica " + str(replicas)
        cmd += " --storage-backend magma --bucket-type couchbase --enable-flush 1"
        systemcmd(cmd)

        for sc in range(0,len(bv["scopes"])) :
            sv = bv["scopes"][sc]
            cmd = "/opt/couchbase/bin/couchbase-cli collection-manage -c " + host + " -u Administrator -p password --bucket " + sv["bc"]
            cmd += " --create-scope " + sv["name"]
            systemcmd(cmd)

            for cc in range(0,len(sv["collections"])) :
                cv = sv["collections"][cc]
                cmd = "/opt/couchbase/bin/couchbase-cli collection-manage -c " + host + " -u Administrator -p password --bucket " + sv["bc"]
                cmd += " --create-collection " + sv["name"] + "." + cv["name"]
                systemcmd(cmd)

    #systemcmd("/opt/couchbase/bin/couchbase-cli rebalance -c localhost -u Administrator -p password")
    
# index creation/build per collection (It will not wait, build maight fail if many queued)

def create_collection_indexes(conn, f, collection):
    for ddlc in collection["ddls"] :
        stmt = {"statement":ddlc, "query_context": collection["sc"]}
        s = ddlc.replace("col",collection["sc"]+".col")
        f.write (s + ";\n")
        n1ql_execute(conn, stmt , None)
    f.write("\n")

# load the data using gocb program and build indexes
# compile using "go build -o load_data main.go" 

def load_data(conn, workload):
    if not cfg["loaddata"] :
       return

    host = cfg["host"].replace("http://","")
    f = open(cfg["indexfile"], "w")
    for b in sorted(workload.keys()):
        bv = workload[b]
        create_javascript_udf(b)
        backupcollection = ""
        for sc in range(0,len(bv["scopes"])) :
            sv = bv["scopes"][sc]
            create_udf(conn, b, sv["bc"]+"."+sv["name"])
            for cc in range(0,len(sv["collections"])) :
                cv = sv["collections"][cc]
                if not cfg["usebackup"] or bv["type"] == "free" or backupcollection == "" :
                    cmd = "./load_data --host " + host + " --username Administrator --password password "
                    cmd += " --batches " + str(cv["batches"]) + " --batch-size " + str(cv["batchsize"])
                    cmd += " --bucket " + b + " --scope " + sv["name"] + " --collection " + cv["name"]
                    systemcmd(cmd)
                    if cfg["usebackup"] and bv["type"] != "free" :
                        backupcollection = b + "." + sv["name"] + "." + cv["name"]
                        systemcmd("/bin/rm -rf " + cfg["backupdir"] + "/*")
                        cmd = "/opt/couchbase/bin/cbbackupmgr config --archive " + cfg["backupdir"]
                        cmd += " --repo default --disable-bucket-config --include-data " + backupcollection
                        systemcmd(cmd)
                        cmd = "/opt/couchbase/bin/cbbackupmgr backup --archive " + cfg["backupdir"]
                        cmd += " --repo default --cluster " + cfg["host"]
                        cmd += " --username Administrator --password password --threads 16 "
                        systemcmd(cmd)
                else :
                     cmd = "/opt/couchbase/bin/cbbackupmgr restore --archive " + cfg["backupdir"]
                     cmd += " --repo default --cluster " + cfg["host"]
                     cmd += " --username Administrator --password password --threads 16 --purge "
                     cmd += " --map-data " + backupcollection +"=" + b + "." + sv["name"] + "." + cv["name"]
                     systemcmd(cmd)
                create_collection_indexes(conn, f, sv["collections"][cc])
    f.close()
    wait_build_indexes(conn)
        
def create_javascript_udf(tenant) :
    cmd = "curl -s -k -X POST "
    cmd = cmd + cfg["host"]
    cmd = cmd +  ":8093/evaluator/v1/libraries/"
    cmd = cmd + tenant
    cmd = cmd + " -u Administrator:password -H 'content-type: application/json'"
    cmd = cmd +  " -d 'function udf(id) { return id}'"
    systemcmd(cmd)

def create_udf(conn, tenant, qc) :
    stmt = {"statement":"CREATE OR REPLACE FUNCTION udf(id) LANGUAGE JAVASCRIPT AS 'udf' AT '" + tenant + "'", "query_context": qc}
    n1ql_execute(conn, stmt , None)

# prepare statements based on workload for all the buckets

def prepare_stmts(conn, workload) :
    for b in sorted(workload.keys()):
        bv = workload[b]
        sqs = []
        for sv in bv["scopes"] :
            for cv in sv["collections"] :
                qc = cv["sc"]
                queryworkloads = cfg["workloads"][cfg["workload"]]
                for k in sorted(queryworkloads.keys()):
                   if queryworkloads[k] == 0 :
                      continue
                   stmt = cfg["aqueries"][k].replace("col00", cv["name"])
                   nindexes = cfg["nindexes"]
                   tximplicit = "UPDATE" in stmt
                   if "ac0" in stmt :
                        nindexes = cfg["naindexes"]
                   for iv in range(0,nindexes) :
                        tv = "c" + str(iv)
                        lstmt = stmt.replace("c0", tv)
                        sq = generate_prepared_query(conn, qc, lstmt)
                        for nc in range(0, queryworkloads[k]) :
                            sqs.append({"name":sq, "qc": qc, "batches":cv["batches"], "batchsize":cv["batchsize"], "tximplicit":tximplicit,
                                        "stmt":lstmt.replace("col",qc+".col"), "qtype": k})
        bv["prepareds"] = sqs
    return

def wait_build_indexes(conn) :
    cnt = 1
    while cnt > 0 :
        cbody = n1ql_execute(conn, {"statement": "SELECT RAW COUNT(1) FROM system:indexes AS s WHERE s.state = 'deferred'"} , None)
        cnt = cbody["results"][0]
        if cnt == 0 :
            break
        bstmt = {"statement":"SELECT k, inames FROM system:indexes AS s LET k = NVL2(s.bucket_id, CONCAT2('.', s.bucket_id, s.scope_id, s.keyspace_id), s.keyspace_id) WHERE s.namespace_id = 'default' GROUP BY k LETTING inames = ARRAY_AGG(s.name) FILTER (WHERE s.state = 'deferred') HAVING ARRAY_LENGTH(inames) > 0"}
        body = n1ql_execute(conn, bstmt , None)
        for i in body["results"] :
            r = body["results"][i]
            bindexes = ""
            for k in r["inames"] :
                 if k != 0 :
                     bindexes += ","
                 bindexes += r["inames"][k]
            print ("BUILD INDEX ON ", k, "(", bindexes, ")")
            n1ql_execute(conn, {"statement": "BUILD INDEX ON " + r["k"] + " (" + bindexes + ")"}, None)
            time.sleep(5)
        time.sleep(5)

    cnt = 1
    while cnt > 0 :
        cbody = n1ql_execute(conn, {"statement": "SELECT RAW COUNT(1) FROM system:indexes AS s WHERE s.state != 'online'"} , None)
        cnt = cbody["results"][0]
        if cnt == 0 :
            break
        print ("WAITING FOR INDEXES READY : ", cnt)
        time.sleep(5)

def tenant_distribution(nthreads, workload):
    tenants = []
    if "querytenantweight" not in cfg["loads"].keys() :
        for b in sorted(workload.keys()) :
            for i in range (0, workload[b]["ncollections"]) :
                tenants.append(b)
        return tenants

    load = cfg["loads"][cfg["load"]]
    querytenantweight = cfg["loads"]["querytenantweight"]
    for k in sorted(load.keys()):
        if load[k] > 0 :
           for b in sorted(workload.keys()):
               if workload[b]["type"] == k :
                  if querytenantweight[k] == 0:
                     tenants.append(b)
                  else :
                     for c in range(0, querytenantweight[k]) :
                        tenants.append(b)
    return tenants

def n1ql_connection(url):    
    conn = urllib3.connection_from_url(url+ ":8093")
    return conn

def n1ql_execute(conn, stmt, posparam):
    stmt['creds'] = '[{"user":"Administrator","pass":"password"}]'
    if posparam:
        stmt['args'] = json.JSONEncoder().encode(posparam)
    response = conn.request('POST', '/query/service', fields=stmt, encode_multipart=False)
    response.read(cache_content=False)
    body = json.loads(response.data.decode('utf8'))
#    print (json.JSONEncoder().encode(body))
    return body

def run_tid(tid, starttime, duration, tenants, workload, result, debug):
    # get all assigned buckets per this thread
    time.sleep(tid*0.1)
    random.seed()
    conn = n1ql_connection(cfg["host"])
    qualifiedbatches = cfg["qualifiedbatches"]

    i = 0
    while (time.time() - starttime) <= duration:
    # pick randome bucket from assigned buckets   
         bucket = random.randint(0,len(tenants)-1)
         bname = tenants[bucket]
    # pick random prepared statement of this bucket (random query/random index combination)
         sqs =  workload[bname]["prepareds"]
         rv = random.randint(0,len(sqs)-1)
         sq = sqs[rv]
         stmt = {'prepared': '"' + sq["name"] + '"'}
#         stmt['scan_consistency'] = 'request_plus'
         if sq["tximplicit"] :
             stmt['tximplicit']= True

         stmt['query_context'] = sq["qc"]
         stmt['$start'] = random.randint(0,sq["batches"]-qualifiedbatches)
         stmt['$end'] = stmt['$start'] + qualifiedbatches - 1
         stmt['$limit'] = int(0.2*sq["batchsize"])
         t0 = time.time()
         body = n1ql_execute(conn, stmt, None) 
         t1 = time.time()
         
         i = i+1
         if body["status"] != "success" :
             params = {"$start":stmt['$start'], "$end": stmt['$end'], "$limit": stmt['$limit'], "bucket": bname, "qtype":sq["qtype"]}
             print ("tid:" , tid, "loop: ", i, json.JSONEncoder().encode(body["metrics"]), json.JSONEncoder().encode(body["errors"]))
         elif tid == 0 and (i%100) == 0 :
             params = {"$start":stmt['$start'], "$end": stmt['$end'], "$limit": stmt['$limit'], "bucket": bname, "qtype": sq["qtype"]}
             print ("tid:" , tid, "loop: ", i, json.JSONEncoder().encode(body["metrics"]), params)

         result[bname][sq["qtype"]]["count"] += 1
         result[bname][sq["qtype"]]["time"] += (t1-t0)
         if "computeUnits" in body.keys() :
             cu = body["computeUnits"]
             for k in cu.keys() :
                  result[bname][sq["qtype"]]["computeUnits"][k] += cu[k]

    return result

def generate_prepared_query(conn, qc, qstring):
    stmt = {'statement': 'PREPARE ' + qstring }
    if qc:
        stmt['query_context'] = qc
    body = n1ql_execute(conn, stmt, None)
    name = str(body['results'][0]['name'])
    return name
    return {'prepared': '"' + name + '"'}
    
def print_workload(f, tenants, workload):
    f.write("----------BEGIN CONFIG ------------\n")
    f.write("    " + json.dumps(cfg))
    f.write("\n----------END CONFIG ------------\n\n\n")
    f.write("----------BEGIN TENANTS WORKLOAD------------\n")
    f.write("    " + json.dumps(tenants))
    f.write("\n----------END TENANTS WORKLOAD------------\n\n\n")
    for b in sorted(workload.keys()):
        f.write("----------BEGIN TENANT '" + b + "' STATEMENTS ------------\n\n")
        prepareds = workload[b]["prepareds"]
        for i in range(0, len(prepareds)):
             f.write("    " + prepareds[i]["qtype"].upper() + " " + prepareds[i]["stmt"] + ";\n")
        f.write("----------END TENANT '" + b + "' STATEMENTS ------------\n\n")

def result_init(tenants, workload):
    results = {}
    for b in sorted(workload.keys()):
        results[b] = {}
        for i in range(0, len(workload[b]["prepareds"])) :
            sd = workload[b]["prepareds"][i]
            results[b][sd["qtype"]] = {"count":0, "time": 0, "computeUnits":{"queryCU":0, "jsCU":0, "gsiRU":0, "ftsRU":0, "kvRU":0, "kvWU": 0}}

    return results 

def result_finish(wfd, results) :
    fbresult = {}
    fqresult = {}

    for i in range(0,len(results)) :
        result = results[i]
        for b in sorted(result.keys()):
            for q in sorted(result[b].keys()):
                if q in sorted(fqresult.keys()):
                    fqresult[q]["count"] += result[b][q]["count"]
                    fqresult[q]["time"] += result[b][q]["time"]
                    for k in fqresult[q]["computeUnits"].keys() :
                        fqresult[q]["computeUnits"][k] += result[b][q]["computeUnits"][k]
                else :
                    fqresult[q] = result[b][q].copy()
            
                if b in sorted(fbresult.keys()) :
                    fbresult[b]["count"] += result[b][q]["count"]
                    fbresult[b]["time"] += result[b][q]["time"]
                    for k in fbresult[b]["computeUnits"].keys() :
                        fbresult[b]["computeUnits"][k] += result[b][q]["computeUnits"][k]
                else :
                    fbresult[b] = result[b][q].copy()

    for b in sorted(fbresult.keys()) :
         if fbresult[b]["count"] != 0:
            fbresult[b]["avg"] = round((fbresult[b]["time"]/fbresult[b]["count"])*1000,3)
         fbresult[b]["time"] = round(fbresult[b]["time"]*1000,3)
         fbresult[b]["cqps"] = round(fbresult[b]["count"]*1000/fbresult[b]["time"], 3)
         fbresult[b]["cqpspc"] = round(fbresult[b]["cqps"]/cfg["ncores"], 3)

    count = 0
    total = 0.0
    cqps = 0.0
    cqpspc = 0.0
    cu = {"queryCU":0, "jsCU":0, "gsiRU":0, "ftsRU":0, "kvRU":0, "kvWU": 0}
    for q in sorted(fqresult.keys()) :
         if fqresult[q]["count"] != 0:
            fqresult[q]["avg"] = round((fqresult[q]["time"]/fqresult[q]["count"])*1000,3)
            count += fqresult[q]["count"]
            total += fqresult[q]["time"]
         for k in cu.keys() :
             cu[k] += fqresult[q]["computeUnits"][k]
         fqresult[q]["time"] = round(fqresult[q]["time"]*1000,3)
         fqresult[q]["cqps"] = round(fqresult[q]["count"]*1000/fqresult[q]["time"], 3)
         fqresult[q]["cqpspc"] = round(fqresult[q]["cqps"]/cfg["ncores"], 3)
         cqps += fqresult[q]["cqps"]
         cqpspc += fqresult[q]["cqpspc"]
    
    duration = cfg["duration"]
    wfd.write("\n\n ---------------BEGIN REQUESTS BY TENANT (ms)-----------\n")
    for b in sorted(fbresult.keys()) :
        wfd.write("    " + b.upper() + " " + json.dumps(fbresult[b]) + "\n")
    wfd.write("\n ---------------END REQUESTS BY TENANT-----------\n")

    wfd.write("\n ---------------BEGIN REQUESTS BY QUERY (ms)-----------\n")
    for q in sorted(fqresult.keys()) :
        wfd.write("    " + q.upper() + " " + json.dumps(fqresult[q]) + "\n")
    wfd.write("\n ---------------END REQUESTS BY QUERY-----------\n\n")
    wfd.write("    TOTAL REQUESTS : " + str(count) +  ",  TOTAL TIME(ms) : " + str(round(total*1000,3)) + ", AVG TIME(ms) : " + str(round((total/count)*1000,3)) + "\n")
    wfd.write("    CQPS : " + str(cqps) + " CQPSPC : " + str(cqpspc) + "\n")
    wfd.write("    SQPS : " + str(round(count/duration,3)) + " SQPSPC : " + str(round(count/(duration*cfg["ncores"]),3)) + "\n")
    wfd.write("    COMPUTE UNITS : " + json.dumps(cu) + "\n\n")
    
def run_execute(conn, wfd, workload) :
    if not cfg["execute"]:
       return
    prepare_stmts(conn, workload)
    nthreads = int((cfg["nthreads"] * int(cfg["load"]))/100)
    tenants = tenant_distribution(nthreads, workload)
    print_workload(wfd, tenants, workload)
    results = {}
    result = result_init(tenants, workload)
    jobs = []
    results = []
    starttime = time.time()
    pool = multiprocessing.Pool(nthreads)
    for tid in range(0, nthreads):
        r = pool.apply_async(run_tid, (tid, starttime, cfg["duration"], tenants, workload, result.copy(), True))
        jobs.append(r)

    pool.close()
    pool.join()

    for j in jobs:
        j.wait()
        results.append(j.get())

    result_finish(wfd, results) 

if __name__ == "__main__":
    wfd = open(cfg["workloadfile"]+".txt", "w")
    #wfd = open(cfg["workloadfile"]+str(uuid.uuid4())+".txt", "w")
    workload = workload_init()
    create_collections(workload)
    conn = n1ql_connection(cfg["host"])
    load_data(conn, workload)
    wfd.write("START TIME : " + str(datetime.datetime.now()) + "\n")
    run_execute(conn, wfd, workload)
    wfd.write("END TIME : " + str(datetime.datetime.now()) + "\n")
    wfd.close()

   
