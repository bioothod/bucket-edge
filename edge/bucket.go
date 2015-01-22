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
	DefragState	int32
}

type EdgeCtl struct {
	Ell *etransport.Elliptics
	Stat *elliptics.DnetStat
	Session *elliptics.Session

	AddressDefragMap map[elliptics.RawAddr]int
	DefragStates map[elliptics.AddressBackend]AbState

	DefragCount int
	Timeback int
}

func EdgeInit(config_file string) (e *EdgeCtl) {
	conf := &config.ProxyConfig {}
	err := conf.Load(config_file)
	if err != nil {
		log.Fatalf("Could not load config %s: %q", config_file, err)
	}

	e = &EdgeCtl {
		AddressDefragMap: make(map[elliptics.RawAddr]int),
		DefragStates: make(map[elliptics.AddressBackend]AbState),
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

	return e
}

func (e *EdgeCtl) BucketCheck(bname string) (err error) {
	b, err := bucket.ReadBucket(e.Ell, bname)
	if err != nil {
		log.Printf("bucket_check: could not read bucket '%s': %v", bname, err)
		return err
	}

	for _, group_id := range b.Meta.Groups {
		sg, ok := e.Stat.Group[group_id]
		if ok {
			b.Group[group_id] = sg
		} else {
			log.Printf("bucket_check: bucket: %s: there is no group %d in stats", bname, group_id)
			return fmt.Errorf("bucket: %s: there is no group %d in stats", bname, group_id)
		}
	}

	return e.BucketDefrag(b)
}
