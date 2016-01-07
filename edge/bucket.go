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

	TmpPath			string
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

	log.Printf("defrag: bucket: %s, %s, free-space-rate: %f, removed-space-rate: %f, queued for defrag\n",
		b.Name, ab.String(), free_space_rate, removed_space_rate)
	return true
}

func (e *EdgeCtl) InsertBucket(b *bucket.Bucket) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	bs := &Bstat {
		Bucket:		b,
		NeedRecovery:	true,
	}

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

	log.Printf("bucket: %s, need-defrag: %d, need-recovery: %v\n", b.Name, bs.NeedDefrag, bs.NeedRecovery)
	e.Buckets[b.Name] = bs
	return
}
