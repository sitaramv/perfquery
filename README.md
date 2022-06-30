go build -o load_data main.go
./load_data --host 127.0.0.1 --username Administrator --password password --batches 1 --batch-size 100 --bucket b0 --scope s0 --collection col0
