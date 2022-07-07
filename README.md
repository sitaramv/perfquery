## Build

go build -o load_data main.go

## Run

./load_data --host 127.0.0.1 --username Administrator --password password --batches 1 --batch-size 100 --bucket b0 --scope s0 --collection col0


## Strategy

#Tenant Types

<pre>
   Free          1M documents  1 scope,   1 collection  Per collection (1M documents, 1 - 128 byte index), 1 - ARRAY (1:3) index 30 bytes)
   Light        10M documents  2 scopes,  1 collection  Per collection (5M documents, 1 - 128 byte index), 1 - ARRAY (1:3) index 30 bytes)
   Moderate     30M documents  2 scopes,  3 collections Per collection (5M documents, 1 - 128 byte index), 1 - ARRAY (1:3) index 30 bytes)
   Overheavy    60M documents  2 scopes,  6 collections Per collection (5M documents, 1 - 128 byte index), 1 - ARRAY (1:3) index 30 bytes)
   Superheavy  120M documents  2 scopes, 12 collections Per collection (5M documents, 1 - 128 byte index), 1 - ARRAY (1:3) index 30 bytes)
</pre>

# Bucket/Tenant Disturbution based on workload

<pre>
   50%        Free  : 5, Light : 8, Moderate :  5, Overheavy: 1, Superheavy : 1
   90%        Free  : 0, Light : 5, Moderate : 10, Overheavy: 4, Superheavy : 1
  100%        Free  : 0, Light : 0, Moderate : 20, Overheavy: 0, Superheavy : 0
</pre>

# Query Tenant Distribution based on workload
<pre>
   Each client picks the tenant based on proptioanl number of collections i.e. (collection in the bucket/total collection in all the buckets)
       Aprox selection : Free: 1, Light: 2, Moderate: 6, Overheavy: 12, Superheavy: 24

   50%     0.5 * Clients  Free  : 5, Light : 8*2, Moderate :  5*6, Overheavy: 1*12, Superheavy : 1*24
   90%     0.9 * Clients  Free  : 0, Light : 5*2, Moderate : 10*6, Overheavy: 4*12, Superheavy : 1*24
  100%     1.0 * Clients  Free  : 0, Light : 0,   Moderate : 20*6, Overheavy: 0,    Superheavy : 0
</pre>

# Query workload

<pre>
    Q0     : All tenants SELECT META(d).id, d.f115, d.xxx FROM col0 AS d USE INDEX(`#sequential`) WHERE d.c0 BETWEEN $start AND $end
    Q1     : All tenants SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end
    Q2     : All tenants SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end ORDER BY d.c0 DESC LIMIT $limit
    Q3     : All tenants SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col0 AS l JOIN col0 AS r USE HASH(BUILD) ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end
    Q4     : All tenants SELECT g1, COUNT(1) AS cnt FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end GROUP BY IMOD(d.id,10) AS g1 ORDER BY g1
    Q5     : All tenants SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE ANY v IN d.a1 SATISFIES v.ac0 BETWEEN $start AND $end AND v.aid = 2 END
    Q6     : All tenants WITH cte AS (SELECT RAW t FROM col0 AS t WHERE t.c0 BETWEEN $start AND $end) SELECT META(l).id, l.f115, l.xxx, r.yyy FROM col0 AS l JOIN cte AS r ON l.id = r.id WHERE l.c0 BETWEEN $start AND $end AND r.c0 BETWEEN $start AND $end
    Q7     : All tenants SELECT META(d).id, d.f115, d.xxx FROM col0 AS d UNNEST d.a1 AS u WHERE u.ac0 BETWEEN $start AND $end AND u.aid = 1
    Q8     : All tenants(tximplicit) UPDATE col0 AS d SET d.comment = d.comment WHERE d.c0 BETWEEN $start AND $end
    Q9     : All tenants (UDF)       SELECT META(d).id, d.f115, d.xxx FROM col0 AS d WHERE d.c0 BETWEEN $start AND $end AND udf(d.c0) = d.c0
    SIMPLE : {"q0":0, "q1":10, "q2":3, "q3":3, "q4":2, "q5":2, "q6":0, "q7":0, "q8":0, "q9":0}
    MEDIUM : {"q0":0, "q1":6, "q2":4, "q3":3, "q4":2, "q5":2, "q6":2, "q7":1, "q8":0, "q9":0}
    COMPLEX: {"q0":0, "q1":5, "q2":3, "q3":2, "q4":2, "q5":2, "q6":2, "q7":2, "q8":1, "q9":1}}

    5% counts as 1. So total must be 20
    Each scope in the tenant
        Each collection in the scope
            Each index in the collection (based on how many indexes want to use)
                Statement is prepared
                    Also repeated the based on query factor number
    Each thread picks one of the prepare statement and execute it
</pre>


## AWS run:
<pre>
   Data Nodes : 3 - c6gd.4xlarge
                Default 8 GB storage gp3
                Add Another storage 500GB  gp3
                    IOPS 15,000
                    Transfer Rate 1000 MiBs

   Index Nodes: 2 - c6gd.4xlarge
                Default 8 GB storage gp3
                Add Another storage 500GB  gp3
                    IOPS 15,000
                    Transfer Rate 1000 MiBs

   Query Nodes: 1 c6gd.4xlarge
                Default 30 GB storage gp3

   Test Nodes: 1 c6gd.4xlarge
                Default 30 GB storage gp3

   Download product file, aws key file to current directory
   Update ec2.sh line hosts, servicenames, keyfile, rpm file
   ./ec2.sh -a -k <keyfile> -r <rpm file>

   login to test machine
   cd perfquery
   Update  cfg object host (query host) and other settings if needed
   python mt.py

</pre>
