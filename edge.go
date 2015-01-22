package main

import (
	"flag"
	"github.com/bioothod/bucket-edge/edge"
	"log"
)

func main() {
	bucket := flag.String("bucket", "", "Bucket name to defrag and recover")
	config_file := flag.String("config", "", "Transport config file")
	defrag_count := flag.Int("dcount", edge.DefragBackendsPerServerDefault,
		"Maximum number of defragmentation processes running in parallel in the bucket")
	flag.Parse()

	if *bucket == "" {
		log.Fatalf("You must specify bucket name")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	e := edge.EdgeInit(*config_file, *defrag_count)

	err := e.BucketCheck(*bucket)
	if err != nil {
		log.Fatalf("Could not check bucket '%s': %v", *bucket, err)
	}

	return
}
