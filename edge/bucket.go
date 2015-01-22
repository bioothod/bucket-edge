package edge

import (
	"fmt"
	"github.com/bioothod/backrunner/config"
	"github.com/bioothod/backrunner/etransport"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
)

type AbState struct {
	DefragState	uint32
}

type EdgeCtl struct {
	ell *etransport.Elliptics
	stat *elliptics.DnetStat
	session *elliptics.Session

	address_defrag_map map[elliptics.RawAddr]int
	defrag_states map[elliptics.AddressBackend]AbState

	defrag_count int
}

func EdgeInit(config_file string, defrag_count int) (e *EdgeCtl) {
	conf := &config.ProxyConfig {}
	err := conf.Load(config_file)
	if err != nil {
		log.Fatalf("Could not load config %s: %q", config_file, err)
	}

	e = &EdgeCtl {
		address_defrag_map: make(map[elliptics.RawAddr]int),
		defrag_states: make(map[elliptics.AddressBackend]AbState),
		defrag_count: defrag_count,
	}

	e.ell, err = etransport.NewEllipticsTransport(conf)
	if err != nil {
		log.Fatalf("Could not create Elliptics transport: %v", err)
	}

	e.session, err = elliptics.NewSession(e.ell.Node)
	if err != nil {
		log.Fatalf("Could not create Elliptics session: %v", err)
	}

	e.stat, err = e.ell.Stat()
	if err != nil {
		log.Fatal("Could not read statistics: %v", err)
	}

	return e
}

func (e *EdgeCtl) BucketCheck(bname string) (err error) {
	b, err := bucket.ReadBucket(e.ell, bname)
	if err != nil {
		log.Printf("bucket_check: could not read bucket '%s': %v", bname, err)
		return err
	}

	for _, group_id := range b.Meta.Groups {
		sg, ok := e.stat.Group[group_id]
		if ok {
			b.Group[group_id] = sg
		} else {
			log.Printf("bucket_check: bucket: %s: there is no group %d in stats", bname, group_id)
			return fmt.Errorf("bucket: %s: there is no group %d in stats", bname, group_id)
		}
	}

	return e.BucketDefrag(b)
}
