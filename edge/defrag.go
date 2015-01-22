package edge

import (
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/elliptics-go/elliptics"
	"log"
	"time"
)

const (
	DefragStateNotRunning uint32 = 0
	DefragStateStarted uint32 = 1

	DefragBackendsPerServer int = 3
)

func (e *EdgeCtl) BucketStatusParse(b *bucket.Bucket, addr *elliptics.RawAddr, ch <-chan *elliptics.DnetBackendsStatus) (int, error) {
	defrag_backends := make([]int32, 0)
	total_backends := 0

	for st := range ch {
		if st.Error != nil {
			return 0, st.Error
		}

		for _, backend_status := range st.Backends {
			total_backends += 1

			ab := elliptics.AddressBackend {
				Addr: *addr,
				Backend: backend_status.Backend,
			}

			if backend_status.DefragState == DefragStateStarted {
				defrag_backends = append(defrag_backends, backend_status.Backend)

				e.defrag_states[ab] = AbState{
					DefragState: DefragStateStarted,
				}
			}

			if backend_status.DefragState == DefragStateNotRunning {
				prev_state, ok := e.defrag_states[ab]
				if ok {
					if prev_state.DefragState == DefragStateStarted {
						delete(e.defrag_states, ab)
					}
				}
			}

			//log.Printf("bucket-status: bucket: %s, %s, defrag_state: %d, defrag_count: %d\n",
			//	b.Name, ab.String(), backend_status.DefragState, defrag_count)
		}
	}

	log.Printf("bucket-status: bucket: %s, address: %s, defrag_count: %d, total_backends: %d, backends: %v\n",
		b.Name, addr.String(), len(defrag_backends), total_backends, defrag_backends)
	return len(defrag_backends), nil
}

func (e *EdgeCtl) BucketStatus(b *bucket.Bucket) (error) {
	for addr, _ := range e.address_defrag_map {
		ch := e.session.BackendsStatus(addr.DnetAddr())
		defrag_count, err := e.BucketStatusParse(b, &addr, ch)
		if err != nil {
			log.Printf("bucket-status: bucket: %s: addr: %s: stat error: %v\n", b.Name, addr.String(), err)
		}

		e.address_defrag_map[addr] = defrag_count
	}

	return nil
}

func (e *EdgeCtl) BucketStartDefrag(b *bucket.Bucket) (err error) {
	err = e.BucketStatus(b)
	if err != nil {
		return fmt.Errorf("bucket-start-defrag: bucket: %s, status error: %v", b.Name, err)
	}

	for ab, state := range e.defrag_states {
		if state.DefragState == DefragStateStarted {
			continue
		}

		defrag_count, ok := e.address_defrag_map[ab.Addr]
		if !ok {
			log.Printf("bucket-start-defrag: bucket: %s, %s: there is no status, not starting defrag",
				b.Name, ab.String())
			continue
		}

		if defrag_count >= DefragBackendsPerServer {
			//log.Printf("bucket-start-defrag: bucket: %s, %s: defrag_count: %d, max: %d: not starting defrag\n",
			//	b.Name, ab.String(), defrag_count, DefragBackendsPerServer)
			continue
		}

		log.Printf("bucket-start-defrag: bucket: %s, %s: starting defrag\n", b.Name, ab.String())
		ch := e.session.BackendStartDefrag(ab.Addr.DnetAddr(), ab.Backend)
		defrag_started, err := e.BucketStatusParse(b, &ab.Addr, ch)
		if err != nil {
			log.Printf("bucket-start-defrag: bucket: %s: %s: reply status error: %v\n", b.Name, ab.String(), err)
		}

		e.address_defrag_map[ab.Addr] = defrag_count + defrag_started
	}

	return
}

func (e *EdgeCtl) BucketDefrag(b *bucket.Bucket) (err error) {
	for group_id, sg := range b.Group {
		e.address_defrag_map = make(map[elliptics.RawAddr]int)

		for ab, sb := range sg.Ab {
			free_space_rate := bucket.FreeSpaceRatio(sb, 0)
			removed_space_rate := float64(sb.VFS.BackendRemovedSize) / float64(sb.VFS.TotalSizeLimit)
			log.Printf("bucket: %s, group: %d, %s: starting defragmentation, used: %d, removed: %d, total: %d, free-space-rate: %f, removed-space-rate: %f",
				b.Name, group_id, ab.String(),
				sb.VFS.BackendUsedSize, sb.VFS.BackendRemovedSize, sb.VFS.TotalSizeLimit,
				free_space_rate, removed_space_rate)

			e.defrag_states[ab] = AbState {
						DefragState: DefragStateNotRunning,
					    }
			e.address_defrag_map[ab.Addr] = 0
		}

		for {
			e.BucketStartDefrag(b)

			if len(e.defrag_states) == 0 {
				break
			}

			time.Sleep(30 * time.Second)
		}
	}

	return
}
