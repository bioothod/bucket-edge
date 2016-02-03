package edge

import (
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
	"time"
)

type Bstat struct {
	Bucket			*bucket.Bucket
	NeedDefrag		int
	NeedRecovery		bool
}

type Hstat struct {
	// number of running defrag/recovery processes on given host
	// worker can not start new defrag/recovery if it equals to limit stored in @EdgeCtl
	DefragSlots		int
	RecoverySlots		int
}

func (e *EdgeCtl) SelectBucketForDefrag() (*Bstat, *elliptics.AddressBackend) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	for bname, bs := range e.Buckets {
		for _, sg := range bs.Bucket.Group {
			for ab, st := range sg.Ab {
				hs, ok := e.Hosts[ab.Addr]
				if !ok {
					log.Printf("defrag: bucket: %s, %s: there is no address in @Hosts map\n",
						bs.Bucket.Name, ab.String())
					continue
				}

				if hs.DefragSlots >= e.DefragCount {
					continue
				}

				// there is no statistics for this group, skip it
				if st.VFS.TotalSizeLimit == 0 {
					log.Printf("defrag: bucket: %s, %s: no statistics for this backend\n",
						bs.Bucket.Name, ab.String())
					continue
				}

				if st.RO {
					log.Printf("defrag: bucket: %s, %s: backend is in read-only mode\n",
						bs.Bucket.Name, ab.String())
					continue
				}

				if bs.NeedDefrag == 0 {
					continue
				}

				if !e.WantDefrag(bs.Bucket, &ab, st) {
					continue
				}

				hs.DefragSlots++
				delete(e.Buckets, bname)

				fmt.Printf("%s: bucket: %s, %s, need-defrag: %d, need-recovery: %v, selected bucket for defrag\n",
					time.Now().String(), bs.Bucket.Name, ab.String(), bs.NeedDefrag, bs.NeedRecovery)
				return bs, &ab
			}
		}
	}

	return nil, nil
}

func (e *EdgeCtl) PutBucketBackFromDefrag(bs *Bstat) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	bs.NeedDefrag--
	log.Printf("bucket: %s, need-defrag: %d, need-recovery: %v\n", bs.Bucket.Name, bs.NeedDefrag, bs.NeedRecovery)

	if bs.NeedRecovery || bs.NeedDefrag > 0 {
		e.Buckets[bs.Bucket.Name] = bs
	}

	fmt.Printf("%s: bucket: %s, need-defrag: %d, need-recovery: %v, defrag completed, buckets left: %d\n",
		time.Now().String(), bs.Bucket.Name, bs.NeedDefrag, bs.NeedRecovery, len(e.Buckets))
}

func (e *EdgeCtl) SetDefragSlots(ab *elliptics.AddressBackend, slots int) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	hs, ok := e.Hosts[ab.Addr]
	if ok {
		hs.DefragSlots = slots
	}

	return
}

func (e *EdgeCtl) StartDefrag() error {
	bs, ab := e.SelectBucketForDefrag()
	if bs == nil {
		return nil
	}
	defer e.PutBucketBackFromDefrag(bs)

	s, err := e.DataSession(bs.Bucket.Meta.Groups)
	if err != nil {
		log.Printf("defag: could not create new session: %v\n", err)
		return err
	}
	defer s.Delete()

	s.BackendStartDefrag(ab.Addr.DnetAddr(), ab.Backend)

	finished := false
	for {
		for status := range s.BackendsStatus(ab.Addr.DnetAddr()) {
			if status.Error != nil {
				log.Printf("defrag: bucket: %s, %s: backend status error: %v\n", bs.Bucket.Name, ab.Addr.String(), err)
				return nil
			}

			defrag_state := elliptics.DefragStateNotStarted
			defrag_slots := 0
			for _, backend := range status.Backends {
				// if backend is not enabled, skip it
				if backend.State != elliptics.BackendStateEnabled {
					continue
				}

				if backend.Backend == ab.Backend {
					if backend.DefragState != elliptics.DefragStateInProgress {
						finished = true
					}

					defrag_state = backend.DefragState
				}

				if backend.DefragState == elliptics.DefragStateInProgress {
					defrag_slots += 1
				}
			}

			log.Printf("start-defrag: bucket: %s, %s: backends being defragmented: %d, defrag state: %s, " +
				"waiting for completion, finished state: %v\n",
				bs.Bucket.Name, ab.String(), defrag_slots, elliptics.DefragStateString[defrag_state], finished)

			e.SetDefragSlots(ab, defrag_slots)
		}

		if finished {
			break
		}

		time.Sleep(30 * time.Second)
	}

	return nil
}

func (e *EdgeCtl) ScanDefragHosts() error {
	for ra, hs := range e.Hosts {
		for status := range e.Session.BackendsStatus(ra.DnetAddr()) {
			if status.Error != nil {
				log.Printf("scan-defrag-hosts: host: %s: backend status error: %v\n", ra.String(), status.Error)
				break
			}

			defrag_slots := 0
			for _, backend := range status.Backends {
				if backend.DefragState == elliptics.DefragStateInProgress {
					defrag_slots += 1
				}
			}

			e.Mutex.Lock()
			hs.DefragSlots = defrag_slots
			e.Mutex.Unlock()

			log.Printf("scan-defrag-hosts: host: %s: backends being defragmented: %d\n", ra.String(), defrag_slots)
		}
	}

	return nil
}
