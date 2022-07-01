#/usr/bin/python

import sys
import os
import math
import json
import time
import random
import requests
import urllib3
import decimal
import multiprocessing

cfg = {
       "loaddata": False,                     # control bucket/scope/collection/index/data drop/creation/load
       "execute": True,                       # control execute queries
       "nthreads" : 15,                       # max number of client threads (might lowered by load setting)
       "host": 'http://172.23.97.79',         # querynode host ip
       "replicas": 0,                         # replica setting
       "memory": 4096,                        # datanode memory  (divided by nbuckets)
       "nbuckets": 4,                         # number of bucktes
       "nscopes"   : 2,                       # number of scopes per bucket
       "dataweightdocs" : 1000000,           # number of docs per each wieght
       "dataweightpercollection" : 5,         # number of wieght per collection
       "nindexes": 1,                         # number of indexes per collection
       "naindexes": 0,                        # number of array indexes per collection
       "workload" : "q1",                     # workload type (see workloads)
       "load":"100",                          # load percent (see loads)
       "batchsize": 100,                      # batchsize (i.e. qualified rows per query)
       "qualifiedbatches": 1,                 # number of batches (to increase qualified rows per query)
       "tcount": 30000,                       # number of requestis per client before stop
       "indexfile": "index.txt",              # index statements
       "indexes"  : [ "CREATE INDEX ix0 IF NOT EXISTS ON col0 (c0, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ix1 IF NOT EXISTS ON col0 (c1, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ix2 IF NOT EXISTS ON col0 (c2, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ix3 IF NOT EXISTS ON col0 (c3, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ix4 IF NOT EXISTS ON col0 (c4, f115) WITH {'defer_build': true}"
                    ],
       "aindexes"  : ["CREATE INDEX ixa0 IF NOT EXISTS ON col0 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac0) FOR v IN a1 END, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ixa1 IF NOT EXISTS ON col0 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac1) FOR v IN a1 END, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ixa2 IF NOT EXISTS ON col0 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac2) FOR v IN a1 END, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ixa3 IF NOT EXISTS ON col0 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac3) FOR v IN a1 END, f115) WITH {'defer_build': true}",
                      "CREATE INDEX ixa4 IF NOT EXISTS ON col0 (ALL ARRAY FLATTEN_KEYS(v.aid, v.ac4) FOR v IN a1 END, f115) WITH {'defer_build': true}"
                    ],
       "aqueries" : {"q0": "SELECT META(d).id, d.f115, d.xxx FROM col0 AS d USE INDEX(`#seqentialscan`) WHERE d.c0 BETWEEN $start AND $end",
                     "q1": "SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end",
                     "q2": "SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end ORDER BY d.c0 DESC LIMIT $limit",
                     "q3": "SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col0 AS l JOIN col0 AS r USE HASH(BUILD) ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end",
                     "q4": "SELECT g1, COUNT(1) AS cnt FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end GROUP BY IMOD(d.id,10) AS g1 ORDER BY g1",
                     "q5": "SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE ANY v IN d.a1 SATISFIES v.ac0 BETWEEN $start AND $end AND v.id = 2 END",
                     "q6": "WITH cte AS (SELECT RAW t FROM col0 AS t WHERE t.c0 BETWEEN $start AND $end) SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col0 AS l JOIN cte AS r ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end",
                     "q7": "SELECT META(d).id, d.f115, d.xxx FROM col0 AS d UNNEST d.a1 AS u WHERE u.ac0 BETWEEN $start AND $end AND u.aid = 1",
                     "q8": "UPDATE col0 AS d SET d.comment = d.comment WHERE d.c0 BETWEEN $start AND $end",
                     "q9": "SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end AND udf() = true"
                    },
      "workloads":{"q0": {"q0":20}, "q1": {"q1":20}, "q2": {"q2":20}, "q3": {"q3":20}, "q4": {"q4":20},
                   "q5": {"q5":20}, "q6": {"q6":20}, "q7": {"q7":20}, "q8": {"q8":20}, "q9": {"q9":20},
                   "simple":{"q0":0, "q1":10, "q2":3, "q3":3, "q4":2, "q5":2, "q6":0, "q7":0, "q8":0, "q9":0},
                   "medium":{"q0":0, "q1":6, "q2":4, "q3":3, "q4":2, "q5":2, "q6":2, "q7":1, "q8":0, "q9":0},
                   "complex":{"q0":0, "q1":5, "q2":3, "q3":2, "q4":2, "q5":2, "q6":2, "q7":2, "q8":1, "q9":1}},
      "loads": { 
              "dataweight":        {"free":1, "light":10, "moderate": 30, "heavy": 60, "superheavy": 60},
              "querytenantweight": {"free":1, "light":5, "moderate": 10, "heavy": 20, "superheavy": 0},
              "50":{"total":20, "free": 3, "light":10, "moderate":5, "heavy":2, "superheavy":0 },
              "90":{"total":20, "free": 0, "light":5, "moderate":10, "heavy":5, "superheavy":0 },
              "100": {"total":20, "free": 0, "light":0, "moderate":0, "heavy":0, "superheavy":20},
               }
      }

def workload_init():
       load = cfg["loads"][cfg["load"]]
       factor = int(cfg["nbuckets"]/load["total"])
       tbatches = 0 
       ad = []
       for k in load.keys() :
           if k == "total" or load[k] == 0 :
               continue
           batches = int((cfg["loads"]["dataweight"][k] * cfg["dataweightdocs"])/cfg["batchsize"])
           for nc in xrange(0, factor * load[k]) :
                ad.append({"type":k, "batches": batches})
                tbatches = tbatches + batches
       batchespercollection = (cfg["dataweightpercollection"]*cfg["dataweightdocs"])/cfg["batchsize"]
       workload = {}
       memory = int(cfg["memory"] - 1024*cfg["nbuckets"])
       if memory < 0 :
           memory = 0
       for bv in xrange(0, cfg["nbuckets"]) :
          bc = "b" + str(bv) 
          workload[bc] = ad[bv].copy()
          bmemory = int(memory*ad[bv]["batches"]/tbatches) + 1024
          workload[bc]["memory"] = bmemory
          scopes = []
          ncollections = int(ad[bv]["batches"]/batchespercollection)
          nscopes = cfg["nscopes"]
          if ncollections == 0 :
              nscopes = 1 
              ncollections = 1
          else :
              ncollections = int(ncollections/nscopes)
          for sv in xrange(0,nscopes) :
             sc = "s" + str(sv)
             qc = bc + "." + sc
             collections = []
             for cv in xrange(0,ncollections) :
                collection = "col" + str(cv)
                bindexes = ""

                ddls = []
                for iv in xrange(0,cfg["nindexes"]) :
                   if bindexes != "" :
                      bindexes = bindexes + ", "
                   bindexes = bindexes + "ix" + str(iv)

                   stmt = cfg["indexes"][iv].replace("col0", collection)
                   ddls.append(stmt)

                for iv in xrange(0,cfg["naindexes"]) :
                   if bindexes != "" :
                      bindexes = bindexes + "," 
                   bindexes = bindexes + "ixa" + str(iv)

                   stmt = cfg["aindexes"][iv].replace("col0", collection)
                   ddls.append(stmt)
                ddls.append("BUILD INDEX ON " + collection + " (" + bindexes + ")")
                batches = int(ad[bv]["batches"]/(ncollections*nscopes))
                collections.append({"sc":qc, "name":collection, "ddls": ddls, "batchsize": cfg["batchsize"], "batches": batches})
             scopes.append({"name": sc, "bc": bc, "collections": collections})
          workload[bc]["batchsize"] = cfg["batchsize"]
          workload[bc]["scopes"] = scopes
       return workload

def systemcmd(cmd) :
    print cmd
    os.system(cmd)

# bucket/scope/collection re-creation

def create_collections(workload):
    if not cfg["loaddata"] :
       return

    host = cfg["host"]
    replicas = cfg["replicas"]
    
    systemcmd("curl -s -u Administrator:password " + host + ":8091/internalSettings -d 'maxBucketCount=80'")
    systemcmd("curl -s -u Administrator:password " + host + ":8091/settings/querySettings -d 'queryCompletedLimit=0'")
    systemcmd("curl -s -u Administrator:password " + host + ":8091/settings/querySettings -d 'queryPreparedLimit=100000'")
    for b in workload.keys():
        systemcmd("/opt/couchbase/bin/couchbase-cli bucket-delete -c " + host + " -u Administrator -p password --bucket " + b )

    for b in workload.keys():
        bv = workload[b]
        cmd = "/opt/couchbase/bin/couchbase-cli bucket-create -c " + host + " -u Administrator -p password --bucket "
        cmd += b
        cmd += " --bucket-ramsize " + str(bv["memory"])
        cmd += " --bucket-replica " + str(replicas)
        cmd += " --storage-backend magma --bucket-type couchbase --enable-flush 1"
        systemcmd(cmd)

        for sc in xrange(0,len(bv["scopes"])) :
            sv = bv["scopes"][sc]
            cmd = "/opt/couchbase/bin/couchbase-cli collection-manage -c " + host + " -u Administrator -p password --bucket " + sv["bc"]
            cmd += " --create-scope " + sv["name"]
            systemcmd(cmd)

            for cc in xrange(0,len(sv["collections"])) :
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
    for b in workload.keys():
        bv = workload[b]
        for sc in xrange(0,len(bv["scopes"])) :
            sv = bv["scopes"][sc]
            for cc in xrange(0,len(sv["collections"])) :
                cv = sv["collections"][cc]
                cmd = "./load_data --host " + host + " --username Administrator --password password "
                cmd += " --batches " + str(cv["batches"]) + " --batch-size " + str(cv["batchsize"])
                cmd += " --bucket " + b + " --scope " + sv["name"] + " --collection " + cv["name"]
                systemcmd(cmd)
                create_collection_indexes(conn, f, sv["collections"][cc])
    f.close()
        
# prepare statements based on workload for all the buckets

def prepare_stmts(conn, workload) :
    for b in workload.keys():
        bv = workload[b]
        sqs = []
        for sv in bv["scopes"] :
            for cv in sv["collections"] :
                qc = cv["sc"]
                queryworkloads = cfg["workloads"][cfg["workload"]]
                for k in queryworkloads.keys() :
                   if queryworkloads[k] == 0 :
                      continue
                   stmt = cfg["aqueries"][k].replace("col0", cv["name"])
                   nindexes = cfg["nindexes"]
                   tximplicit = "UPDATE" in stmt
                   if "ac0" in stmt :
                        nindexes = cfg["naindexes"]
                   for iv in xrange(0,nindexes) :
                        tv = "c" + str(iv)
                        lstmt = stmt.replace("c0", tv)
                        sq = generate_prepared_query(conn, qc, lstmt)
                        for nc in xrange(0, queryworkloads[k]) :
                            sqs.append({"name":sq, "qc": qc, "batches":cv["batches"], "batchsize":cv["batchsize"], "tximplicit":tximplicit})
        bv["prepareds"] = sqs
    return

def tenant_distribution(nthreads, workload):
    tenants = []
    load = cfg["loads"][cfg["load"]]
    querytenantweight = cfg["loads"]["querytenantweight"]
    for k in load.keys() :
        if k != "total" and load[k] > 0 :
           for b in workload.keys():
               if workload[b]["type"] == k :
                  if querytenantweight[k] == 0:
                     tenants.append(b)
                  else :
                     for c in xrange(0, querytenantweight[k]) :
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
#    print json.JSONEncoder().encode(body)
    return body


def run_tid(tid, count, tenants, workload, debug):
    # get all assigned buckets per this thread
    time.sleep(tid*0.1)
    random.seed()
    conn = n1ql_connection(cfg["host"])
    qualifiedbatches = cfg["qualifiedbatches"]

    for i in xrange (0, count):
    # pick randome bucket from assigned buckets   
         bucket = random.randint(0,len(tenants)-1)
         bv =  workload[tenants[bucket]]
    # pick random prepared statement of this bucket (random query/random index combination)
         sqs =  bv["prepareds"]
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
         body = n1ql_execute(conn, stmt, None) 
         
         if body["status"] != "success" :
             params = {"$start":stmt['$start'], "$end": stmt['$end'], "$limit": stmt['$limit'], "bucket": tenants[bucket]}
             print "tid:" , tid, "loop: ", i, json.JSONEncoder().encode(body["metrics"]), json.JSONEncoder().encode(body["errors"])
         elif tid == 0 and (i%100) == 0 :
             params = {"$start":stmt['$start'], "$end": stmt['$end'], "$limit": stmt['$limit'], "bucket": tenants[bucket]}
             print "tid:" , tid, "loop: ", i, json.JSONEncoder().encode(body["metrics"]), params

def generate_prepared_query(conn, qc, qstring):
    stmt = {'statement': 'PREPARE ' + qstring }
    if qc:
        stmt['query_context'] = qc
    body = n1ql_execute(conn, stmt, None)
    name = str(body['results'][0]['name'])
    return name
    return {'prepared': '"' + name + '"'}
    
def run_execute(conn, workload) :
    if not cfg["execute"]:
       return
    prepare_stmts(conn, workload)
    nthreads = int((cfg["nthreads"] * int(cfg["load"]))/100)
    tenants = tenant_distribution(nthreads, workload)
    jobs = []
    for tid in xrange(0, nthreads):
        j = multiprocessing.Process(target=run_tid, args=(tid, cfg["tcount"], tenants, workload, True))
        jobs.append(j)
        j.start()
   
    for j in jobs:
        j.join()

if __name__ == "__main__":
    workload = workload_init()
    conn = n1ql_connection(cfg["host"])
    create_collections(workload)
    load_data(conn, workload)
    run_execute(conn, workload)

   
