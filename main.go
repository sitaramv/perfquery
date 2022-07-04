package main

import (
	"flag"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

type Ndoc struct {
	Aid      int    `json:"aid"`
	Ac0      int    `json:"ac0"`
	Ac1      int    `json:"ac1"`
	Ac2      int    `json:"ac2"`
	Ac3      int    `json:"ac3"`
	Ac4      int    `json:"ac4"`
}

type Doc struct {
	Id      int    `json:"id"`
	C0      int    `json:"c0"`
	C1      int    `json:"c1"`
	C2      int    `json:"c2"`
	C3      int    `json:"c3"`
	C4      int    `json:"c4"`
	F115    string `json:"f115"`
	F245    string `json:"f245"`
        A1      []Ndoc `json:"a1"`
	Comment string `json:"comment"`
}

func loadColl(col *gocb.Collection, batches, batchSize int, document *Doc) {
        options := &gocb.UpsertOptions{Timeout:10*time.Second}
	for j := 0; j < batches; j++ {
		var wg sync.WaitGroup
		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func(id, k, id1 int) {
				doc := *document
				doc.Id = id1
				doc.C0 = id
				doc.C1 = id
				doc.C2 = id
				doc.C3 = id
				doc.C4 = id
                                for pos:= 0; pos <  len(doc.A1); pos ++ {
				    doc.A1[pos].Aid = pos
				    doc.A1[pos].Ac0 = id
				    doc.A1[pos].Ac1 = id
				    doc.A1[pos].Ac2 = id
				    doc.A1[pos].Ac3 = id
				    doc.A1[pos].Ac4 = id
                                }

				_, err := col.Upsert(fmt.Sprintf("k%09d", k), &doc, options)
				if err != nil {
					log.Printf("failed to upsert test data %v", err)
				}
				wg.Done()
			}(j, i+(batchSize*j), i)
		}
		wg.Wait()
	}
}

func genTestData(addr, user, pass , bname, sname, cname string, batches, batchSize int) {
	connStr := fmt.Sprintf("couchbase://%s", addr)
	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
	})
	if err != nil {
		panic(err)
	}

	bucket := cluster.Bucket(bname)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
        }
	scope := bucket.Scope(sname)
	collection := scope.Collection(cname)

	var f115 strings.Builder
	var f245 strings.Builder
	var comment strings.Builder
	f115.WriteString("f")
	f245.WriteString("f")
	comment.WriteString("comment")
	for i := 0; i < 114; i++ {
		f115.WriteString("1")
	}
	for i := 0; i < 245; i++ {
		f245.WriteString("1")
	}
	for i := 0; i < 425; i++ {
		comment.WriteString("-")
	}
	document := &Doc{
		F115:    f115.String(),
		F245:    f245.String(),
                A1:      make([]Ndoc,3),
		Comment: comment.String(),
	}

        loadColl(collection, batches, batchSize, document)
}

func main() {
	addr := flag.String("host", "127.0.0.1", "The address to connect to")
	username := flag.String("username", "Administrator", "The username to use")
	password := flag.String("password", "password", "The password to use")
	batches := flag.Int("batches", 1, "Number of batches used for testing")
	batchSize := flag.Int("batch-size", 100, "Batch size")
	bucketName := flag.String("bucket", "b0", "Default bucket")
	scopeName := flag.String("scope", "s0", "Default scope")
	collectionName := flag.String("collection", "col0", "Default collection")

	flag.Parse()

        mv := make(map[string]interface{}, 10)
        mv["batches"] = *batches
        mv["batch-size"] = *batchSize
        mv["docs"] = (*batchSize)*(*batches)
        mv["bucket"] = *bucketName
        mv["scope"] = *scopeName
        mv["collection"] = *collectionName
        buf, _ := json.Marshal(mv)

	log.Printf("Loading Data... %s",string(buf)) 
	genTestData(*addr, *username, *password, *bucketName, *scopeName, *collectionName, *batches, *batchSize)
}
