package main

import (
	"flag"
	"github.com/bioothod/bucket-edge/edge"
	"log"
)

func main() {
	bname := flag.String("bucket", "", "Bucket name to defrag and recover")
	config_file := flag.String("config", "", "Transport config file")
	defrag_option := flag.Bool("defrag", false, "Run bucket defragmentation")
	recovery_option := flag.Bool("recovery", false, "Run bucket recovery")
	defrag_count := flag.Int("dcount", edge.DefragBackendsPerServerDefault,
		"Maximum number of defragmentation processes running in parallel in the bucket")
	tm := flag.Int("timeback", 0,
		"The gap in seconds back from current time. If backend defragmentation was completed within this gap, do not run it again")
	tmp_path := flag.String("tmp-path", "", "Path where all temporal objects will be stored")
	flag.Parse()

	if *bname == "" {
		log.Fatalf("You must specify bucket name")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	if *recovery_option && *tmp_path == "" {
		log.Fatalf("Recovery option requires temporal path")
	}

	e := edge.EdgeInit(*config_file)

	e.Timeback = *tm
	e.DefragCount = *defrag_count
	e.TmpPath = *tmp_path

	b, err := e.GetBucket(*bname)
	if err != nil {
		log.Printf("Could not get bucket '%s': %v\n", *bname, err)
		return
	}

	if *defrag_option {
		err := e.BucketDefrag(b)
		if err != nil {
			log.Printf("Could not check bucket '%s': %v\n", *bname, err)
		}
	}

	if *recovery_option {
		err := e.BucketRecovery(b)
		if err != nil {
			log.Printf("Could not check bucket '%s': %v\n", *bname, err)
		}
	}


	return
}
