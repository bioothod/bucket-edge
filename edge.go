package main

import (
	"bufio"
	"flag"
	"github.com/bioothod/bucket-edge/edge"
	"log"
	"time"
	"os"
	"sync"
)

const (
	DefragBackendsPerServerDefault int = 3
	DefragFreeRateDefault float64 = 0.4
	DefragRemovedRateDefault float64 = 0.1
	NumWorkersDefault int = 30
)

func main() {
	bfile := flag.String("buckets", "", "File with bucket names to defrag and recover, one name per line")
	config_file := flag.String("config", "", "Transport config file")
	defrag_count := flag.Int("defrag-count", DefragBackendsPerServerDefault,
		"Maximum number of defragmentation or recovery processes running in parallel on a single node")
	defrag_free_rate := flag.Float64("defrag-free-rate", DefragFreeRateDefault,
		"Defragmentation will only start if backend's free rate is less than this value")
	defrag_removed_rate := flag.Float64("defrag-removed-rate", DefragRemovedRateDefault,
		"Defragmentation will only start if backend's removed rate is more than this value")
	tm := flag.Int("timeback", 60 * 60 * 24 * 7,
		"The gap in seconds back from current time. If backend defragmentation or recovery was completed within this gap, do not run it again")
	workers := flag.Int("workers", NumWorkersDefault, "Maximum number of defrag/recovery workers per cluster")
	tmp_path := flag.String("tmp-path", "", "Path where all temporal objects will be stored")
	flag.Parse()

	if *bfile == "" {
		log.Fatalf("You must specify file with bucket names")
	}

	if *config_file == "" {
		log.Fatal("You must specify config file")
	}

	if *tmp_path == "" {
		log.Fatalf("You must specify temporal path")
	}

	e := edge.EdgeInit(*config_file)

	e.Timeback = time.Now().Add(-time.Duration(*tm) * time.Second)
	e.DefragCount = *defrag_count
	e.DefragFreeRate = *defrag_free_rate
	e.DefragRemovedRate = *defrag_removed_rate
	e.TmpPath = *tmp_path

	r, err := os.Open(*bfile)
	if err != nil {
		log.Fatalf("Could not open file '%s': %v\n", *bfile, err)
	}
	defer r.Close()

	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		bname := scanner.Text()

		b, err := e.GetBucket(bname)
		if err != nil {
			log.Printf("Could not get bucket '%s': %v\n", bname, err)
			continue
		}

		e.InsertBucket(b)
	}

	e.ScanHosts()

	if err = scanner.Err(); err != nil {
		log.Fatalf("Error reading file '%s': %v\n", *bfile, err)
	}

	if len(e.Buckets) == 0 {
		log.Fatalf("Could not load any bucket, exiting\n")
	}

	var wait sync.WaitGroup
	for idx := 0; idx < *workers; idx++ {
		wait.Add(1)

		go func() {
			defer wait.Done()

			for {
				err = e.Run()
				if err != nil {
					return
				}

				time.Sleep(time.Second)
			}

		}()
	}

	wait.Wait()
	return
}
