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

				hs, ok := e.Hosts[ab.Addr]
				if !ok {
					log.Printf("defrag: bucket: %s, %s: there is no address in @Hosts map\n",
						bs.Bucket.Name, ab.String())
					continue
				}

				if hs.DefragSlots == e.DefragCount {
					continue
				}

				hs.DefragSlots++
				delete(e.Buckets, bname)
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
	log.Printf("bucket: %s, need-defrag: %d, need-recovery: %s\n", b.Name, bs.NeedDefrag, bs.NeedRecovery)

	if bs.NeedRecovery || bs.NeedDefrag > 0 {
		e.Buckets[bs.Bucket.Name] = bs
		return
	}

	log.Printf("bucket: %s, recovered and defragmented\n", bs.Bucket.Name)
}

func (e *EdgeCtl) SelectBucketForRecovery() (*Bstat) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	for bname, bs := range e.Buckets {
		if bs.NeedRecovery {
			delete(e.Buckets, bname)
			return bs
		}
	}

	return nil
}

func (e *EdgeCtl) PutBucketBackFromRecovery(bs *Bstat) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()

	bs.NeedRecovery = false
	log.Printf("bucket: %s, need-defrag: %d, need-recovery: %s\n", b.Name, bs.NeedDefrag, bs.NeedRecovery)

	if bs.NeedDefrag > 0 {
		e.Buckets[bs.Bucket.Name] = bs
		return
	}

	log.Printf("bucket: %s, recovered and defragmented\n", bs.Bucket.Name)
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
				log.Printf("defrag: bucket: %s, %s: backend status error: %v\n", bs.Bucket.Name, ab.String(), err)
				return nil
			}

			defrag_slots := 0
			for _, backend := range status.Backends {
				if backend.Backend == ab.Backend {
					finished = true
					if backend.State != elliptics.BackendStateEnabled {
						continue
					}

					if backend.DefragState == elliptics.DefragStateInProgress {
						defrag_slots += 1
						finished = false
					}
				}
			}

			e.SetDefragSlots(ab, defrag_slots)
		}

		if finished {
			break
		}

		time.Sleep(10 * time.Second)
	}

	return nil
}

func (e *EdgeCtl) StartRecovery() error {
	bs := e.SelectBucketForRecovery()
	if bs == nil {
		return nil
	}
	defer e.PutBucketBackFromRecovery(bs)

	e.BucketRecovery(bs.Bucket)
	return nil
}

func (e *EdgeCtl) Run() error {
	if len(e.Buckets) == 0 {
		return fmt.Errorf("there are no buckets to run defrag/recovery")
	}

	err := e.StartDefrag()
	if err != nil {
		return err
	}

	err = e.StartRecovery()
	if err != nil {
		return err
	}

	return nil
}
