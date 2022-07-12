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
       "host": 'http://ec2-34-223-103-134.us-west-2.compute.amazonaws.com',       # querynode host ip
       "workload" : "complex",                # workload type (see workloads)
       "load":"90",                          # load percent (see loads)
       "loaddata": False,                      # control bucket/scope/collection/index/data drop/creation/load
       "execute": False,                      # control execute queries
       "nthreads" : 85,                       # max number of client threads (might lowered by load setting)
       "datareplicas": 2,                     # data replica setting
       "indexreplicas": 1,                    # data ndex replicas setting
       "memory": 24576,                       # datanode memory  (divided by nbuckets)
       "loadoverhead": 1024,                  # loading over head allocation
       "nbuckets": 20,                        # number of bucktes
       "nscopes"   : 2,                       # number of scopes per bucket
       "dataweightdocs" : 1000000,            # number of docs per each wieght
       "dataweightpercollection" : 5,         # number of wieght per collection
       "nindexes": 1,                         # number of indexes per collection
       "naindexes": 1,                        # number of array indexes per collection
       "indextype": 0,                        # index type 0 or 1 see indexes
       "batchsize": 100,                      # batchsize (i.e. qualified rows per query)
       "qualifiedbatches": 1,                 # number of batches (to increase qualified rows per query)
       "duration": 600,                       # execution duration in seconds
       "ncores": 16,                           # number of query service cores
       "indexfile": "index.txt",              # index statements
       "workloadfile": "workload",            # workload statements
       "backupdir":"/data/backups",           # backup dir
       "usebackup": False,                    # use backup/restore vs load_data
       "staged": True,                       # load using staged approach
       "storage": {"type":"magma", "minsize":1024},  # storage type and minimum size 
      #"storage": {"type":"couchstore", "minsize":100},
       "speical": "",                         # "changereplica", "rebuildindexes"
       "indexes"  : [ "CREATE INDEX ix_c0_cid IF NOT EXISTS ON col00 (c0, cid, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ix_cid_c0 IF NOT EXISTS ON col00 (cid, c0, f115) WITH {'defer_build': true, 'num_replica': indexreplicas }"
                    ],
       "aindexes"  : ["CREATE INDEX ix_apos_ac0_aid IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.apos, v.ac0, v.aid) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }",
                      "CREATE INDEX ix_apos_aid_ac0 IF NOT EXISTS ON col00 (ALL ARRAY FLATTEN_KEYS(v.apos, v.aid, v.ac0) FOR v IN a1 END) WITH {'defer_build': true, 'num_replica': indexreplicas }"
                    ],
       "aqueries" : {"q00": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d USE INDEX(`#sequential`) WHERE d.c0 BETWEEN $start AND $end AND d.cid BETWEEN $istart AND $iend",
                     "q01": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end AND d.cid BETWEEN $istart AND $iend",
                     "q02": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end AND d.cid BETWEEN $istart AND $iend ORDER BY d.c0 DESC LIMIT $limit",
                     "q03": "SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col00 AS l JOIN col00 AS r USE HASH(BUILD) ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end AND l.cid BETWEEN $istart AND $iend AND r.cid BETWEEN $istart AND $iend",
                     "q04": "SELECT g1, COUNT(1) AS cnt FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end AND d.cid BETWEEN $istart AND $iend GROUP BY IMOD(d.id,10) AS g1 ORDER BY g1",
                     "q05": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE ANY v IN d.a1 SATISFIES v.ac0 BETWEEN $start AND $end AND v.apos = 2 AND v.aid BETWEEN $istart AND $iend END",
                     "q06": "WITH cte AS (SELECT RAW t FROM col00 AS t WHERE t.c0 BETWEEN $start AND $end AND t.cid BETWEEN $istart AND $iend) SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col00 AS l JOIN cte AS r ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end AND l.cid BETWEEN $istart AND $iend AND r.cid BETWEEN $istart AND $iend ",
                     "q07": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d UNNEST d.a1 AS u WHERE u.ac0 BETWEEN $start AND $end AND u.apos = 1 AND u.aid BETWEEN $istart AND $iend",
                     "q08": "UPDATE col00 AS d SET d.comment = d.comment WHERE d.c0 BETWEEN $start AND $end AND d.cid BETWEEN $istart AND $iend",
                     "q09": "SELECT META(d).id, d.f115, d.xxx FROM col00 AS d WHERE d.c0 BETWEEN $start AND $end AND udf(d.c0) = d.c0 AND d.cid BETWEEN $istart AND $iend"
                    },
      "workloads":{"q00": {"q00":20}, "q01": {"q01":20}, "q02": {"q02":20}, "q03": {"q03":20}, "q04": {"q04":20},
                   "q05": {"q05":20}, "q06": {"q06":20}, "q07": {"q07":20}, "q08": {"q08":20}, "q09": {"q09":20},
                   "simple":{"q00":0, "q01":10, "q02":3, "q03":3, "q04":2, "q05":2, "q06":0, "q07":0, "q08":0, "q09":0},
                   "medium":{"q00":0, "q01":6, "q02":4, "q03":3, "q04":2, "q05":2, "q06":2, "q07":1, "q08":0, "q09":0},
                   "complex":{"q00":0, "q01":5, "q02":3, "q03":2, "q04":2, "q05":2, "q06":2, "q07":2, "q08":1, "q09":1}},
      "loads": { 
              "dataweight":        {"free":1, "light":10, "moderate": 30, "overheavy": 60, "superheavy": 120},
#             "querytenantweight": {"free":1, "light":2, "moderate": 6, "overheavy": 12, "superheavy": 24}, # default no of collections 1, 2, 6, 12, 24
              "50":{"free": 5, "light":8, "moderate":5, "overheavy":1, "superheavy":1 },
              "90":{"free": 0, "light":5, "moderate":10, "overheavy":4, "superheavy":1 },
              "100": {"free": 0, "light":0, "moderate":20, "overheavy":0, "superheavy":0 }
               },
      "cus": { "q00": {"queryCU":0, "jsCU":0, "gsiRU": 0, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "q01": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "q02": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "q03": {"queryCU":0, "jsCU":0, "gsiRU":26, "ftsRU":0, "kvRU":200, "kvWU": 0},
               "q04": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "q05": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "q06": {"queryCU":0, "jsCU":0, "gsiRU":26, "ftsRU":0, "kvRU":200, "kvWU": 0},
               "q07": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "q08": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 100},
               "q09": {"queryCU":0, "jsCU":0, "gsiRU":13, "ftsRU":0, "kvRU":100, "kvWU": 0},
               "000": {"queryCU":0, "jsCU":0, "gsiRU":0, "ftsRU":0, "kvRU":0, "kvWU": 0},

             }
      }

def workload_init():
       load = cfg["loads"][cfg["load"]]
       staged = cfg["staged"]
       loadoverhead = cfg["loadoverhead"]
       if not staged :
           loadoverhead  = 0
       ntenants = 0
       tbatches = 0
       for k in sorted(load.keys()) :
           ntenants = ntenants + load[k]
       factor = int(cfg["nbuckets"]/ntenants)
       ad = []
       ototal = 0
       for k in reversed(sorted(load.keys())) :
           if load[k] == 0 :
               continue
           batches = int((cfg["loads"]["dataweight"][k] * cfg["dataweightdocs"])/cfg["batchsize"])
           for nc in range(0, factor * load[k]) :
                ad.append({"type":k, "batches": batches})
                tbatches += batches
                if k == "overheavy" :
                   ototal = ototal + 1
                elif k == "superheavy" :
                   ototal = ototal + 4
       batchespercollection = (cfg["dataweightpercollection"]*cfg["dataweightdocs"])/cfg["batchsize"]
       workload = {}
       minsize = cfg["storage"]["minsize"]
       memory = int(cfg["memory"] - minsize*cfg["nbuckets"] - loadoverhead)
       if memory < 0 :
           memory = 0
       for bv in range(0, cfg["nbuckets"]) :
          bc = "b" + str(bv).zfill(2)
          workload[bc] = ad[bv].copy()
          bmemory = minsize
          if minsize == 1024 :
              if ad[bv]["type"] == "overheavy" :
                  bmemory += int(memory/ototal)
              elif ad[bv]["type"] == "superheavy" :
                  bmemory += 4*int(memory/ototal)
          else :
              bmemory + int(memory*ad[bv]["batches"]/tbatches)
          workload[bc]["memory"] = bmemory
          workload[bc]["loadoverhead"] = loadoverhead
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

                ddls = {"0": {"drop":[], "create":[], "build":[]},
                        "1": {"drop":[], "create":[], "build":[]}}

                replicas = str(cfg["indexreplicas"])
                for i in range (0, len(cfg["indexes"])) :
                     bindexes = ""
                     oistmt  = cfg["indexes"][i]
                     istmt = cfg["indexes"][i].replace("col00", collection).replace("indexreplicas",replicas)
                     for iv in range(0,cfg["nindexes"]) :
                         stmt = istmt.replace("c0", "c" + str(iv))
                         iname = istmt[istmt.find("ix_"):istmt.find(" IF")]
                         ddls[str(i)]["drop"].append("DROP INDEX " + iname + " IF EXISTS ON " + collection)
                         ddls[str(i)]["create"].append(stmt)
                         if bindexes != "" :
                            bindexes = bindexes + ", "
                         bindexes = bindexes + iname
                     oistmt  = cfg["aindexes"][i]
                     istmt = cfg["aindexes"][i].replace("col00", collection).replace("indexreplicas",replicas)
                     for iv in range(0,cfg["naindexes"]) :
                         stmt = istmt.replace("c0", "c" + str(iv))
                         iname = istmt[istmt.find("ix_"):istmt.find(" IF")]
                         ddls[str(i)]["drop"].append("DROP INDEX " + iname + " IF EXISTS ON " + collection)
                         ddls[str(i)]["create"].append(stmt)
                         if bindexes != "" :
                            bindexes = bindexes + ", "
                         bindexes = bindexes + iname
                     ddls[str(i)]["build"].append("BUILD INDEX ON " + collection + " (" + bindexes + ")")
                batches = int(ad[bv]["batches"]/(ncollections*nscopes))
                collections.append({"sc":qc, "bname":bc, "sname":sc, "name":collection, "ddls": ddls, "batchsize": cfg["batchsize"], "batches": batches})
             scopes.append({"name": sc, "bc": bc, "collections": collections})
          workload[bc]["batchsize"] = cfg["batchsize"]
          workload[bc]["scopes"] = scopes
       return workload

def systemcmd(cmd) :
    print (cmd)
    os.system(cmd)

def bucket_memory(workload, bname, flag):
    overhead =  workload[bname]["loadoverhead"]
    if overhead <= 0 :
        return

    host = cfg["host"]
    if flag :
        cmd = "/opt/couchbase/bin/couchbase-cli bucket-edit -c " + host + " -u Administrator -p password --bucket " + bname 
        cmd += " --bucket-ramsize " + str(workload[bname]["memory"] + overhead)
        systemcmd(cmd)
    else :
        while overhead > 0 :
            if overhead > 128 :
               overhead -= 128    
            else :
               overhead = 0
            cmd = "/opt/couchbase/bin/couchbase-cli bucket-edit -c " + host + " -u Administrator -p password --bucket " + bname 
            cmd += " --bucket-ramsize " + str(workload[bname]["memory"] + overhead)
            systemcmd(cmd)
            time.sleep(10)

# bucket/scope/collection re-creation

def create_collections(workload):
    if not cfg["loaddata"] :
       return

    host = cfg["host"]
    replicas = cfg["datareplicas"]
    staged = cfg["staged"]
    if staged :
        replicas = 0
    
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
        cmd += " --storage-backend " + cfg["storage"]["type"] + " --bucket-type couchbase --enable-flush 1"
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

# index creation/build per collection (It will not wait, build maight fail if many queued)

def ddl_wrapper(conn, stmt, qc, f, flag):
    if f and flag:
        f.write (stmt.replace("col", qc+".col") + ";\n")
    n1ql_execute(conn, {"statement":stmt, "query_context": qc} , None)

def create_collection_indexes(conn, collection, indextype, drop, create, build, f):
    if drop :
        for ddlc in collection["ddls"][str(0)]["drop"] :
            ddl_wrapper(conn, ddlc, collection["sc"], f, indextype == 0 )

        for ddlc in collection["ddls"][str(1)]["drop"] :
            ddl_wrapper(conn, ddlc, collection["sc"], f, indextype == 1)
        
    if create :
        for ddlc in collection["ddls"][str(indextype)]["create"] :
            ddl_wrapper(conn, ddlc, collection["sc"], f, True)

    if build :
        for ddlc in collection["ddls"][str(indextype)]["build"] :
            ddl_wrapper(conn, ddlc, collection["sc"], f, True)

    if f:
        f.write("\n")

def change_replica(workload):
    host = cfg["host"]
    replicas = cfg["datareplicas"]
    for b in sorted(workload.keys()):
        cmd = "/opt/couchbase/bin/couchbase-cli bucket-edit -c " + host + " -u Administrator -p password --bucket " + b 
        cmd += " --bucket-replica " + str(replicas)
        systemcmd(cmd)
    cmd = "/opt/couchbase/bin/couchbase-cli rebalance -c " + host + " -u Administrator -p password"
    systemcmd(cmd)

def wait_build_indexes(conn, collections, indextype, f):
    for i in range(0, len(collections)):
        cv = collections[i]
        retry = wait_build_indexes_collection(conn, cv)
        if retry :
            create_collection_indexes(conn, cv, indextype, False, False, True, f)
            retry = wait_build_indexes_collection(conn, cv)

def rebuild_indexes(conn, workload, asnew, f):
    collections = []
    indextype  = cfg["indextype"]
    for b in sorted(workload.keys()):
        bv = workload[b]
        for sc in range(0,len(bv["scopes"])) :
            sv = bv["scopes"][sc]
            for cc in range(0,len(sv["collections"])) :
                cv = sv["collections"][cc]
                if asnew : 
                    create_collection_indexes(conn, cv, indextype, asnew, asnew, False, f)
                collections.append(cv)
    n = 0
    bcolletions = []
    for i in range(0, len(collections)):
        cv = collections[i]
        create_collection_indexes(conn, cv, indextype, False, False, True, f)
        bcolletions.append(cv)
        n += 1
        if n == 5 :
            n = 0
            wait_build_indexes(conn, bcolletions, indextype, f)
            bcolletions = []
    wait_build_indexes(conn, bcolletions, indextype, f)

def ddl_wrapper(conn, stmt, qc, f, flag):
    if f and flag:
        f.write (stmt.replace("col", qc+".col") + ";\n")
    n1ql_execute(conn, {"statement":stmt, "query_context": qc} , None)



# load the data using gocb program and build indexes
# compile using "go build -o load_data main.go" 

def load_data(conn, workload):
    if not cfg["loaddata"] :
       return

    host = cfg["host"].replace("http://","")
    f = open(cfg["indexfile"], "w")
    indextype  = cfg["indextype"]
    staged = cfg["staged"]
    for b in sorted(workload.keys()):
        bv = workload[b]
        create_javascript_udf(b)
        backupcollection = ""
        bucket_memory(workload, b, True)
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
                     cmd += " --map-data " + backupcollection + "=" + b + "." + sv["name"] + "." + cv["name"]
                     systemcmd(cmd)
                if not staged :
                     create_collection_indexes(conn, sv["collections"][cc], indextype, True, True, True, f)
        bucket_memory(workload, b, False)
    f.close()
    rebuild_indexes(conn, workload, staged, None)
    if staged :
        change_replica(workload)
        
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
#    n1ql_execute(conn, {"statement":"DELETE FROM system:prepareds"} , None)
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

def notused_wait_build_indexes(conn) :
    cnt = 1
    while cnt > 0 :
        cbody = n1ql_execute(conn, {"statement": "SELECT RAW COUNT(1) FROM system:indexes AS s WHERE s.name != '#sequentialscan' AND s.state IN ['deferred','error']"} , None)
        cnt = cbody["results"][0]
        if cnt == 0 :
            break
        bstmt = {"statement":"SELECT k, inames FROM system:indexes AS s LET k = NVL2(s.bucket_id, CONCAT2('.', s.bucket_id, s.scope_id, s.keyspace_id), s.keyspace_id) WHERE s.name != '#sequentialscan' AND s.namespace_id = 'default' GROUP BY k LETTING inames = ARRAY_AGG(s.name) FILTER (WHERE s.state IN ['deferred', 'error']) HAVING ARRAY_LENGTH(inames) > 0"}
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
        cbody = n1ql_execute(conn, {"statement": "SELECT RAW COUNT(1) FROM system:indexes AS s WHERE s.name != '#sequentialscan' AND s.state != 'online'"} , None)
        cnt = cbody["results"][0]
        if cnt == 0 :
            break
        print ("WAITING FOR INDEXES READY : ", cnt)
        time.sleep(5)

def wait_build_indexes_collection(conn, collection) :
    stmt = "SELECT RAW COUNT(1) FROM system:indexes AS s"
    stmt += " WHERE s.namespace_id = 'default' AND s.name != '#sequentialscan' AND s.state != 'online'" 
    stmt += " AND s.bucket_id = '" + collection["bname"]  + "'"
    stmt += " AND s.scope_id = '" + collection["sname"]  + "'"
    stmt += " AND s.keyspace_id = '" + collection["name"]  + "'"
    c = collection["bname"] + "." + collection["sname"]  + "." + collection["name"]
    for i in range(0, 10):
        cbody = n1ql_execute(conn, {"statement": stmt} , None)
        cnt = cbody["results"][0]
        if cnt == 0 :
            return False
        print ("WAITING FOR " + c  + " INDEXES ONLINE : " + str(cnt))
        time.sleep(30)
    return True

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
         stmt['$istart'] = 0
         stmt['$iend'] = (qualifiedbatches * sq["batchsize"]) - 1
         stmt['$limit'] = int(0.2*sq["batchsize"])
         t0 = time.time()
         body = n1ql_execute(conn, stmt, None) 
         t1 = time.time()
         
         i = i+1
         if body["status"] != "success" :
             params = {"$start":stmt['$start'], "$end": stmt['$end'], "$limit": stmt['$limit'], '$istart':stmt['$istart'], '$iend':stmt['$iend'],"bucket": bname, "qtype":sq["qtype"]}
             print ("tid:" , tid, "loop: ", i, json.JSONEncoder().encode(body["metrics"]), json.JSONEncoder().encode(body["errors"]))
         elif tid == 0 and (i%100) == 0 :
             params = {"$start":stmt['$start'], "$end": stmt['$end'], "$limit": stmt['$limit'], '$istart':stmt['$istart'], '$iend':stmt['$iend'],"bucket": bname, "qtype":sq["qtype"]}
             print ("tid:" , tid, "loop: ", i, json.JSONEncoder().encode(body["metrics"]), params)

         result[bname][sq["qtype"]]["count"] += 1
         result[bname][sq["qtype"]]["time"] += (t1-t0)
         cu = cfg["cus"][sq["qtype"]]
         if "computeUnits" in body.keys() :
             cu = body["computeUnits"]
         for k in cu.keys() :
             result[bname][sq["qtype"]]["CU"][k] += cu[k]

    return result

def generate_prepared_query(conn, qc, qstring):
    stmt = {'statement': 'PREPARE FORCE ' + qstring }
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
    cu = cfg["cus"]["000"].copy()
    for b in sorted(workload.keys()):
        results[b] = {}
        for i in range(0, len(workload[b]["prepareds"])) :
            sd = workload[b]["prepareds"][i]
            if sd["qtype"] not in results[b].keys() :
                 results[b][sd["qtype"]] = {"count":0, "time": 0, "CU":cu.copy()}

    return results 

def result_finish(wfd, results) :
    fbresult = {}
    fqresult = {}
    duration = cfg["duration"]
    cu = cfg["cus"]["000"].copy()

    for i in range(0,len(results)) :
        result = results[i]
        for b in sorted(result.keys()):
            for q in sorted(result[b].keys()):
                if q in sorted(fqresult.keys()):
                    fqresult[q]["count"] += result[b][q]["count"]
                    fqresult[q]["time"] += result[b][q]["time"]
                    for k in fqresult[q]["CU"].keys() :
                        fqresult[q]["CU"][k] += result[b][q]["CU"][k]
                else :
                    fqresult[q] = result[b][q].copy()
                    fqresult[q]["CU"] = result[b][q]["CU"].copy()
            
                if b in sorted(fbresult.keys()) :
                    fbresult[b]["count"] += result[b][q]["count"]
                    fbresult[b]["time"] += result[b][q]["time"]
                    for k in fbresult[b]["CU"].keys() :
                        fbresult[b]["CU"][k] += result[b][q]["CU"][k]
                else :
                    fbresult[b] = result[b][q].copy()
                    fbresult[b]["CU"] = result[b][q]["CU"].copy()

    for b in sorted(fbresult.keys()) :
         if fbresult[b]["count"] != 0:
            fbresult[b]["avg"] = str(round((fbresult[b]["time"]/fbresult[b]["count"])*1000,3)) + "ms"
         fbresult[b]["time"] = str(round(fbresult[b]["time"],3)) + "s"
         fbresult[b]["qps"] = round(fbresult[b]["count"]/duration, 3)
         fbresult[b]["qpspc"] = round(fbresult[b]["qps"]/cfg["ncores"], 3)
         fbresult[b]["CUPS"] = {}
         for ck in fbresult[b]["CU"].keys() :
             if fbresult[b]["CU"][ck] == 0 :
                 del fbresult[b]["CU"][ck]
                 continue
             fbresult[b]["CUPS"][ck] = round(fbresult[b]["CU"][ck]/duration,3)

    count = 0
    total = 0.0
    qps = 0.0
    qpspc = 0.0
    for q in sorted(fqresult.keys()) :
         if fqresult[q]["count"] != 0:
            fqresult[q]["avg"] = str(round((fqresult[q]["time"]/fqresult[q]["count"])*1000,3)) + "ms"
            count += fqresult[q]["count"]
            total += fqresult[q]["time"]
         fqresult[q]["time"] = str(round(fqresult[q]["time"],3)) + "s"
         fqresult[q]["qps"] = round(fqresult[q]["count"]/duration, 3)
         fqresult[q]["qpspc"] = round(fqresult[q]["qps"]/cfg["ncores"], 3)
         fqresult[q]["CUPS"] = {}
         for ck in fqresult[q]["CU"].keys() :
             if fqresult[q]["CU"][ck] == 0 :
                 del fqresult[q]["CU"][ck]
                 continue
             fqresult[q]["CUPS"][ck] = round(fqresult[q]["CU"][ck]/duration,3)
             cu[ck] += fqresult[q]["CU"][ck]

    cus = {}
    cusb = {}
    for k in cu.keys() :
        if cu[k] > 0 :
            cus[k] = round(cu[k]/duration,3)
            cusb[k] = round(cu[k]/(duration*len(fbresult)),3)

    for k in cu.keys() :
        if cu[k] == 0 :
            del cu[k]

    wfd.write("\n\n ---------------BEGIN REQUESTS BY TENANT-----------\n")
    wfd.write(json.dumps(fbresult,sort_keys=True, indent=4))
    wfd.write("\n ---------------END REQUESTS BY TENANT-----------\n")

    wfd.write("\n ---------------BEGIN REQUESTS BY QUERY-----------\n")
    wfd.write(json.dumps(fqresult,sort_keys=True, indent=4))
    wfd.write("\n ---------------END REQUESTS BY QUERY-----------\n\n")

    summary = {}
    summary["tenants"] = len(fbresult)
    summary[cfg["load"]] = cfg["loads"][cfg["load"]]
    summary[cfg["workload"]] = cfg["workloads"][cfg["workload"]]
    summary["clients"] = cfg["nthreads"]
    summary["duration"] = str(duration) + "s"
    summary["cores"] = cfg["ncores"]
    summary["dataweight"] = cfg["loads"]["dataweight"].copy()
    for k in cfg["loads"]["dataweight"].keys() :
         summary["dataweight"][k] *= cfg["dataweightdocs"] * cfg["dataweightpercollection"]
    summary["REQUESTS TOTAL COUNT"] = count
    summary["REQUESTS TOTAL TIME"] = str(round(total,3)) + "s"
    if count > 0 :
        summary["REQUESTS AVG TIME"] = str(round(total*1000/count,3)) + "ms"
    summary["REQUESTS/Second"] = round(count/duration,3)
    summary["REQUESTS/Second/Core"] = round(count/(duration*cfg["ncores"]),3)
    summary["Compute Uints"] = cu
    summary["Compute Uints Per Second"] = cus
    summary["Compute Uints Per Second Per Tenant"] = cusb

    wfd.write("\n ---------------BEGIN WORKLOAD SUMMARY -----------\n")
    wfd.write(json.dumps(summary,sort_keys=True,indent=4))
    wfd.write("\n ---------------END WORKLOAD SUMMARY -----------\n\n")
    
def run_execute(conn, wfd, workload) :
    if not cfg["execute"]:
       return
    wfd.write("START TIME : " + str(datetime.datetime.now()) + "\n")
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
    wfd.write("END TIME : " + str(datetime.datetime.now()) + "\n")

if __name__ == "__main__":
    print("START TIME : " + str(datetime.datetime.now()) + "\n")
    workload = workload_init()
    conn = n1ql_connection(cfg["host"])
    if cfg["speical"] == "changereplica":
        change_replica(workload)
    elif cfg["speical"] == "rebuildindexes":
        f = open(cfg["indexfile"], "w")
        rebuild_indexes(conn, workload, True, f)
        f.close()
    else :
        ext = cfg["load"] + "_" + cfg["workload"] + "_" + str(cfg["dataweightdocs"]) + "_" + str(cfg["indextype"]) + "-"
        ext += str(datetime.datetime.now()).replace(" ","T")
        wfd = open(cfg["workloadfile"]+ext+".txt", "w")
        create_collections(workload)
        load_data(conn, workload)
        run_execute(conn, wfd, workload)
        wfd.close()
        print("END TIME : " + str(datetime.datetime.now()) + "\n")

   
