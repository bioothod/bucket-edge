package edge

import (
	"fmt"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
	"sync"
	"time"
)

type EdgeCtl struct {
	Ell *etransport.Elliptics
	Stat *elliptics.DnetStat
	Session *elliptics.Session

	Mutex	sync.Mutex
	Buckets map[string]*Bstat
	Hosts	map[elliptics.RawAddr]*Hstat

	DefragFreeRate		float64
	DefragRemovedRate	float64
	DefragCount		int
	Timeback		time.Time
	WriteTimeout		int

	TmpPath			string

	Skip			string
}

func EdgeInit(config_file string) (e *EdgeCtl) {
	conf := &config.ProxyConfig {}
	err := conf.Load(config_file)
	if err != nil {
		log.Fatalf("Could not load config %s: %q", config_file, err)
	}

	e = &EdgeCtl {
		Buckets: make(map[string]*Bstat),
		Hosts: make(map[elliptics.RawAddr]*Hstat),
	}

	e.Ell, err = etransport.NewEllipticsTransport(conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	e.Session, err = elliptics.NewSession(e.Ell.Node)
	if err != nil {
		log.Fatalf("Could not create Elliptics session: %v", err)
	}

	e.Stat, err = e.Ell.Stat()
	if err != nil {
		log.Fatal("Could not read statistics: %v", err)
	}

	e.Session.GetRoutes(e.Stat)

	return e
}

func (e *EdgeCtl) GetBucket(bname string) (*bucket.Bucket, error) {
	b, err := bucket.ReadBucket(e.Ell, bname)
	if err != nil {
		log.Printf("get-bucket: could not read bucket '%s': %v\n", bname, err)
		return nil, err
	}

	for _, group_id := range b.Meta.Groups {
		sg, ok := e.Stat.Group[group_id]
		if ok {
			b.Group[group_id] = sg
		} else {
			log.Printf("bucket_check: bucket: %s: there is no group %d in stats", bname, group_id)
			return nil, fmt.Errorf("bucket: %s: there is no group %d in stats", bname, group_id)
		}
	}

	return b, nil
}

func (e *EdgeCtl) WantDefrag(b *bucket.Bucket, ab *elliptics.AddressBackend, st *elliptics.StatBackend) bool {
	free_space_rate := bucket.FreeSpaceRatio(st, 0)
	if free_space_rate > e.DefragFreeRate {
		log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, must be < %f\n",
			b.Name, ab.String(), free_space_rate, e.DefragFreeRate)
		return false
	}

	removed_space_rate := float64(st.VFS.BackendRemovedSize) / float64(st.VFS.TotalSizeLimit)
	if removed_space_rate < e.DefragRemovedRate {
		log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f, must be > %f\n",
			b.Name, ab.String(), free_space_rate,
			removed_space_rate, e.DefragRemovedRate)
		return false
	}

	if free_space_rate > 1 || removed_space_rate > 1 {
		log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f, invalid stats\n",
			b.Name, ab.String(), free_space_rate, removed_space_rate)
		return false
	}

	if st.DefragCompletionStatus == 0 {
		if st.DefragCompletionTime.After(e.Timeback) {
			log.Printf("defrag: bucket: %s, %s: do not need defrag since previous time it was completed at: %s, " +
				"must be completed before: %s\n",
				b.Name, ab.String(), st.DefragCompletionTime.String(), e.Timeback.String())
			return false
		}
	} else {
		log.Printf("defrag: bucket: %s, %s: completion status: %d, time: %s\n",
			b.Name, ab.String(), st.DefragCompletionStatus, st.DefragCompletionTime.String())
	}

	log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f, " +
		"last completion status: %d, time: %s, completed before timeback: %s, queued for defrag\n",
		b.Name, ab.String(), free_space_rate, removed_space_rate,
		st.DefragCompletionStatus, st.DefragCompletionTime.String(), e.Timeback.String())
	return true
}

func (e *EdgeCtl) InsertBucket(b *bucket.Bucket, insert_new bool) {
	bs := &Bstat {
		Bucket:		b,
	}
	bs.NeedRecovery = e.WantRecovery(bs)

	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	for _, sg := range b.Group {
		for ab, st := range sg.Ab {
			_, ok := e.Hosts[ab.Addr]
			if !ok {
				e.Hosts[ab.Addr] = &Hstat{}
			}

			if e.WantDefrag(b, &ab, st) {
				bs.NeedDefrag += 1
			}
		}
	}

	if e.Skip == "defrag" {
		bs.NeedDefrag = 0
	}
	if e.Skip == "recovery" {
		bs.NeedRecovery = false
	}

	old_bs, ok := e.Buckets[b.Name]
	if ok {
		*old_bs = *bs

		// remove bucket if it doesn't need defragmentation or recovery
		if bs.NeedDefrag == 0 && !bs.NeedRecovery {
			delete(e.Buckets, b.Name)
		}
	} else if insert_new && (bs.NeedDefrag > 0 || bs.NeedRecovery) {
		e.Buckets[b.Name] = bs
	}

	log.Printf("insert-bucket: bucket: %s, need-defrag: %d, need-recovery: %v\n", b.Name, bs.NeedDefrag, bs.NeedRecovery)
	return
}

func (e *EdgeCtl) Run() (err error) {
	if len(e.Buckets) == 0 {
		return fmt.Errorf("there are no buckets to run defrag/recovery")
	}

	err = e.StartDefrag()
	if err != nil {
		return err
	}

	err = e.StartRecovery()
	if err != nil {
		return err
	}

	return nil
}

func (e *EdgeCtl) update_stats(bnames []string, insert_new bool) error {
	for _, bname := range bnames {
		b, err := e.GetBucket(bname)
		if err != nil {
			log.Printf("Could not get bucket '%s': %v\n", bname, err)
			continue
		}

		e.InsertBucket(b, insert_new)
	}

	e.ScanDefragHosts()

	return nil
}

func (e *EdgeCtl) InitStats(bnames []string) error {
	return e.update_stats(bnames, true)
}

func (e *EdgeCtl) UpdateStats() error {
	bnames := make([]string, 0)
	e.Mutex.Lock()
	for bname, _ := range e.Buckets {
		bnames = append(bnames, bname)
	}
	e.Mutex.Unlock()

	return e.update_stats(bnames, false)
}
