package edge

import (
	"encoding/gob"
	"fmt"
	"github.com/bioothod/backrunner/bucket"
	"github.com/bioothod/elliptics-go/elliptics"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type DnetIteratorResponseByKey []*elliptics.DnetIteratorResponse
func (a DnetIteratorResponseByKey) Len() int {
	return len(a)
}
func (a DnetIteratorResponseByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a DnetIteratorResponseByKey) Less(i, j int) bool {
	return (&a[i].Key).Less(&a[j].Key)
}

func KeyLess(r1, r2 *elliptics.DnetIteratorResponse) bool {
	return (&r1.Key).Less(&r2.Key)
}
func KeyEqual(r1, r2 *elliptics.DnetIteratorResponse) bool {
	return (&r1.Key).Equal(&r2.Key)
}


type ChunkReader struct {
	last		*elliptics.DnetIteratorResponse
	last_valid	bool
	decoder		*gob.Decoder
}

func NewChunkReader(path string) (*ChunkReader, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(in)

	return &ChunkReader {
		last:		nil,
		last_valid:	false,
		decoder:	dec,
	}, nil
}

func (ch *ChunkReader) Pop() (*elliptics.DnetIteratorResponse, error) {
	if ch.last_valid {
		ch.last_valid = false
		return ch.last, nil
	}

	var resp elliptics.DnetIteratorResponse

	err := ch.decoder.Decode(&resp)
	return &resp, err
}

func (ch *ChunkReader) Push(id *elliptics.DnetIteratorResponse) {
	ch.last = &elliptics.DnetIteratorResponse {
		ID:		id.ID,
		Key:		*elliptics.NewDnetRawID(),
		Status:		id.Status,
		Timestamp:	id.Timestamp,
		UserFlags:	id.UserFlags,
		Size:		id.Size,
		IteratedKeys:	id.IteratedKeys,
		TotalKeys:	id.TotalKeys,
		Flags:		id.Flags,
	}
	copy(ch.last.Key.ID, id.Key.ID)
	ch.last_valid = true
	return
}

type RecoveryEntry struct {
	resp	*elliptics.DnetIteratorResponse
	dst	[]uint32
}

type DnetIteratorResponseByPosition []*RecoveryEntry
func (a DnetIteratorResponseByPosition) Len() int {
	return len(a)
}
func (a DnetIteratorResponseByPosition) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a DnetIteratorResponseByPosition) Less(i, j int) bool {
	return a[i].resp.IteratedKeys < a[j].resp.IteratedKeys
}



type IteratorCtl struct {
	ab		*elliptics.AddressBackend

	gi		*GroupIteratorCtl

	in		<-chan elliptics.IteratorResult
	tmp_dir		string
	err		error

	index		int		// sequence number among all controls in given group

	total_keys	uint64
	chunk_id	int

	keys_to_recover	uint64

	iter_id		uint64

	readers		map[int]*ChunkReader
	empty		bool

	rkeys		[]*RecoveryEntry

	good, bad	uint64
}

func (gi *GroupIteratorCtl) NewIteratorCtl(ab *elliptics.AddressBackend, index int) (*IteratorCtl, error) {
	ctl := &IteratorCtl {
		tmp_dir:	path.Join(gi.tmp_dir, ab.String()),
		ab:		ab,
		index:		index,
		readers:	make(map[int]*ChunkReader),
		empty:		true,
		rkeys:		make([]*RecoveryEntry, 0),
		gi:		gi,
	}

	err := os.MkdirAll(ctl.tmp_dir, os.ModeDir | 0755)
	if err != nil {
		log.Printf("new-iterator-ctl: could not create dir '%s': %v\n", ctl.tmp_dir, err)
		return nil, err
	}

	return ctl, nil
}

func (ctl *IteratorCtl) WriteChunk(ch []*elliptics.DnetIteratorResponse) error {
	if len(ch) == 0 {
		return nil
	}

	sort.Sort(DnetIteratorResponseByKey(ch))

	tmp_path := path.Join(ctl.tmp_dir, strconv.Itoa(ctl.chunk_id))
	out, err := os.OpenFile(tmp_path, os.O_RDWR | os.O_TRUNC | os.O_CREATE, 0644)
	if err != nil {
		log.Printf("write-chunk: bucket: %s, %s: could not open tmp file '%s': %v\n",
			ctl.gi.bucket.Name, ctl.ab.String(), tmp_path, err)
		return err
	}

	enc := gob.NewEncoder(out)
	for _, resp := range ch {
		err = enc.Encode(resp)
		if err != nil {
			log.Printf("write-chunk: bucket: %s, %s: could not encode chunk %d: %v\n",
				ctl.gi.bucket.Name, ctl.ab.String(), ctl.chunk_id, err)
			return err
		}
	}

	reader, err := NewChunkReader(tmp_path)
	if err != nil {
		log.Printf("write-chunk: bucket: %s, %s: could not create chunk reader for file '%s': %v\n",
			ctl.gi.bucket.Name, ctl.ab.String(), tmp_path, err)
		return err
	}

	ctl.readers[ctl.chunk_id] = reader
	ctl.empty = false

	ctl.chunk_id++
	ctl.total_keys += uint64(len(ch))

	return nil
}

func (ctl *IteratorCtl) ReadIteratorResponse() error {
	max_idx := 102400
	idx := 0
	chunk := make([]*elliptics.DnetIteratorResponse, max_idx, max_idx)

	for ir := range ctl.in {
		ctl.iter_id = ir.ID()

		if ir.Error() != nil {
			log.Printf("read-iterator-response: bucket: %s, %s: error: %v\n", ctl.gi.bucket.Name, ctl.ab.String(), ir.Error())
			return ir.Error()
		}

		iresp := ir.Reply()
		// setting response ID to iterator index
		iresp.ID = uint64(ctl.index)

		chunk[idx] = iresp
		idx++
		if idx % 10240 == 0 {
			log.Printf("read-iterator-response: bucket: %s, %s: %d/%d, chunks: %d\n",
				ctl.gi.bucket.Name, ctl.ab.String(), iresp.IteratedKeys, iresp.TotalKeys, ctl.chunk_id)
		}

		if idx == max_idx {
			idx = 0

			err := ctl.WriteChunk(chunk)
			if err != nil {
				return err
			}

			log.Printf("read-iterator-response: bucket: %s, %s: %d/%d, chunks: %d\n",
				ctl.gi.bucket.Name, ctl.ab.String(), iresp.IteratedKeys, iresp.TotalKeys, ctl.chunk_id)
		}
	}

	ctl.WriteChunk(chunk[0:idx])

	log.Printf("read-iterator-response: bucket: %s, %s: completed keys: %d, chunks: %d\n",
		ctl.gi.bucket.Name, ctl.ab.String(), ctl.total_keys, ctl.chunk_id)

	return nil
}

func (ctl *IteratorCtl) PopResponseIterator() (min *elliptics.DnetIteratorResponse, min_idx int, err error) {
	min_idx = -1
	for k, dec := range ctl.readers {
		tmp, err := dec.Pop()
		if err != nil {
			if err != io.EOF {
				log.Printf("pop-response-iterator: bucket: %s, %s: chunk: %d, pop-error: %v\n", ctl.gi.bucket.Name, ctl.ab.String(), k, err)
			} else {
				log.Printf("pop-response-iterator: bucket: %s, %s: chunk: %d, chunk has been processed\n", ctl.ab.String(), k)
			}

			delete(ctl.readers, k)
			continue
		}

		if min_idx == -1 {
			min = tmp
			min_idx = k
			continue
		}

		if KeyLess (tmp, min){
			ctl.readers[min_idx].Push(min)
			min = tmp
			min_idx = k
		} else {
			ctl.readers[k].Push(tmp)
		}
	}

	if min_idx != -1 {
		//log.Printf("pop-response-iterator: %s: chunk: %d, key: %s\n", ctl.ab.String(), min_idx, min.Key.String())
		return min, min_idx, nil
	}

	return nil, -1, err
}

func (ctl *IteratorCtl) PushResponse(resp *elliptics.DnetIteratorResponse, idx int) {
	ctl.readers[idx].Push(resp)
}


func same_groups(gg1, gg2 []uint32) bool {
	if len(gg1) != len(gg2) {
		return false
	}

	for _, g1 := range gg1 {
		has := false

		for _, g2 := range gg2 {
			if g1 == g2 {
				has = true
				break
			}
		}

		if !has {
			return false
		}
	}

	return true
}

type destination struct {
	keys		[]elliptics.DnetRawID
	groups		[]uint32
	ssend		<-chan elliptics.IteratorResult

	failed		[]elliptics.DnetIteratorResponse

	really_failed	[]elliptics.DnetIteratorResponse
}

func NewDestination (groups []uint32) (*destination, error) {
	return &destination {
		keys:		make([]elliptics.DnetRawID, 0),
		groups:		groups,
		failed:		make([]elliptics.DnetIteratorResponse, 0),
		really_failed:	make([]elliptics.DnetIteratorResponse, 0),
	}, nil
}
func (d *destination) Free() {
}

func (d *destination) SameGroups(groups []uint32) bool {
	return same_groups(d.groups, groups)
}

func (d *destination) ReadServerSendResults() (good, bad uint64, err error) {
	for ir := range d.ssend {
		err = ir.Error()

		if err != nil {
			bad++

			if ir.Reply() != nil {
				log.Printf("read-server-send-results: failed key: %s, position: %d/%d, status: %d\n",
					ir.Reply().Key.String(), ir.Reply().IteratedKeys, ir.Reply().TotalKeys, ir.Reply().Status)
			}

			continue
		}

		// special case, 'ping' reply, needed to show progress and to say client that iterator is alive and hasn't timed out
		if ir.Reply().Status == 1 {
			log.Printf("read-server-send-results: key: %s, position: %d/%d, status: %d\n",
				ir.Reply().Key.String(), ir.Reply().IteratedKeys, ir.Reply().TotalKeys, ir.Reply().Status)
			continue
		}

		if ir.Reply().Status < 0 {
			bad++

			if ir.Reply().Status < 0 {
				log.Printf("read-server-send-results: failed key: %s, position: %d/%d, status: %d\n",
					ir.Reply().Key.String(), ir.Reply().IteratedKeys, ir.Reply().TotalKeys, ir.Reply().Status)

				d.failed = append(d.failed, *ir.Reply())
			}

			continue
		}

		good++
	}

	return
}

// if true, key will be removed
func key_is_dead(session *elliptics.Session, fail *elliptics.DnetIteratorResponse, key *elliptics.Key) bool {
	// there is a key in the index file, but it does not exist in the data
	if fail.Status == -int(syscall.ENOENT) {
		return true
	}

	// invalid checksum, read the key, check timestamp, it should be somewhat valid,
	// for example positive, in this century and so on
	if fail.Status == -int(syscall.EILSEQ) {
		bad_key := true
		for wd := range session.Lookup(key) {
			if wd.Error() != nil {
				continue
			}

			info := wd.Info()

			// invalid timestamp, if there will be no valid timestamp, remove this key
			if info.Mtime.Unix() < 0 || info.Mtime.Unix() > time.Now().Unix() + 100000 {
				continue
			}

			bad_key = false
		}

		return bad_key
	}

	return false
}

func (ctl *IteratorCtl) FixupReadWrite(dest *destination) (err error) {
	if len(dest.failed) == 0 {
		return nil
	}

	// we need source group here, i.e. the one,
	// where iteration ran and then we tried to recover those keys into @dst groups
	src, err := ctl.gi.edge.DataSession([]uint32{ctl.gi.group_id})
	if err != nil {
		return nil
	}
	defer src.Delete()

	dst, err := ctl.gi.edge.DataSession(dest.groups)
	if err != nil {
		return nil
	}
	defer dst.Delete()

	key, err := elliptics.NewKey()
	if err != nil {
		return fmt.Errorf("fixup-read-write: could not create key: %v", err)
	}
	defer key.Free()

	rs, err := elliptics.NewEmptyReadSeeker()
	if err != nil {
		return err
	}
	defer rs.Free()

	ws, err := elliptics.NewEmptyWriteSeeker()
	if err != nil {
		return err
	}
	defer ws.Free()

	for idx, fail := range(dest.failed) {
		src.SetIOflags(0)
		dst.SetGroups(dest.groups)

		// set no-checksum flag if this key could not be recovered because of failed checksum
		if fail.Status == -int(syscall.EILSEQ) {
			src.SetIOflags(elliptics.DNET_IO_FLAGS_NOCSUM)

			// if there is a checksum problem and key looks valid (it has been checked in @key_is_dead() function),
			// we overwrite key in source group too to generate new correct checksum
			all_groups := dst.GetGroups()
			all_groups = append(all_groups, ctl.gi.group_id)
			dst.SetGroups(all_groups)
		}

		key.SetRawId(fail.Key.ID)

		err = rs.SetKey(src, key)
		if err != nil {
			log.Printf("fixup-read-write: %d/%d bucket: %s, %s: index: %d, key: %s, timestamp: %s, " +
				"to-copy: %v -> %v, size: %d, src set-key error: %v\n",
				idx, len(dest.failed),
				ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, fail.Key.String(), fail.Timestamp.String(),
				src.GetGroups(), dst.GetGroups(), fail.Size, err)

			dest.really_failed = append(dest.really_failed, fail)
			continue
		}

		dst.SetTimestamp(rs.Mtime)

		err = ws.SetKey(dst, key, 0, rs.TotalSize, rs.TotalSize)
		if err != nil {
			log.Printf("fixup-read-write: %d/%d bucket: %s, %s: index: %d, key: %s, timestamp: %s, " +
				"to-copy: %v -> %v, size: %d/%d, dst set-key error: %v\n",
				idx, len(dest.failed),
				ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, fail.Key.String(), rs.Mtime.String(),
				src.GetGroups(), dst.GetGroups(), fail.Size, rs.TotalSize, err)

			dest.really_failed = append(dest.really_failed, fail)
			continue
		}

		n, err := io.CopyN(ws, rs, int64(rs.TotalSize))
		if err != nil {
			log.Printf("fixup-read-write: %d/%d bucket: %s, %s: index: %d, key: %s, timestamp: %s, " +
				"copied: %v -> %v, copied-size: %d, total-size: %d, copy error: %v\n",
				idx, len(dest.failed),
				ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, fail.Key.String(), rs.Mtime.String(),
				src.GetGroups(), dst.GetGroups(), n, rs.TotalSize, err)

			dest.really_failed = append(dest.really_failed, fail)
		} else {
			log.Printf("fixup-read-write: %d/%d bucket: %s, %s: index: %d, key: %s, timestamp: %s, copied: %v -> %v, size: %d\n",
				idx, len(dest.failed),
				ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, fail.Key.String(), rs.Mtime.String(),
				src.GetGroups(), dst.GetGroups(), rs.TotalSize)
		}
	}

	return nil
}

func (ctl *IteratorCtl) Fixup(dest []*destination) (err error) {
	for _, dst := range dest {
		var good, bad int64

		// we need source group here, i.e. the one,
		// where iteration ran and then we tried to recover those keys into @dst groups
		src, err := ctl.gi.edge.DataSession([]uint32{ctl.gi.group_id})
		if err != nil {
			return nil
		}
		defer src.Delete()

		remove_key, err := elliptics.NewKey()
		if err != nil {
			return fmt.Errorf("could not create remove key: %v", err)
		}
		defer remove_key.Free()

		read_write := make([]elliptics.DnetIteratorResponse, 0)

		for _, fail := range(dst.failed) {
			remove_key.SetRawId(fail.Key.ID)

			if key_is_dead(src, &fail, remove_key) {
				errors := make([]error, 0)

				for r := range src.RemoveKey(remove_key) {
					if r.Error() != nil {
						errors = append(errors, r.Error())
					}
				}

				if len(errors) != 0 {
					bad += 1
					log.Printf("fixup: bucket: %s, %s: index: %d, key: %s: could not remove key from groups: %v, errors: %v\n",
						ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, (&fail.Key).String(), src.GetGroups(), errors)

					dst.really_failed = append(dst.really_failed, fail)
				} else {
					good += 1
					log.Printf("fixup: bucket: %s, %s: index: %d, key: %s: removed key from groups: %v\n",
						ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, (&fail.Key).String(), src.GetGroups())
				}
			} else {
				read_write = append(read_write, fail)
			}
		}

		log.Printf("fixup: bucket: %s, %s: index: %d: destination %v, " +
			"keys: %d, removed: %d, failed to remove: %d, scheduled for read-write fixup: %d\n",
			ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, dst.groups, len(dst.failed), good, bad, len(read_write))

		dst.failed = read_write
		err = ctl.FixupReadWrite(dst)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ctl *IteratorCtl) StartRecovery() (err error) {
	sort.Sort(DnetIteratorResponseByPosition(ctl.rkeys))

	dest := make([]*destination, 0)

	for _, k := range ctl.rkeys {
		inserted := false
		for _, dst := range dest {
			if dst.SameGroups(k.dst) {
				//log.Printf("start-recovery: %s -> %v\n", k.resp.Key.String(), dst.groups)
				dst.keys = append(dst.keys, k.resp.Key)
				inserted = true
				break
			}
		}

		if !inserted {
			dst, err := NewDestination(k.dst)
			if err != nil {
				log.Printf("start-recovery: bucket: %s, %s, index: %d, dst-groups: %v: could not create new destination: %v\n",
					ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, k.dst, err)
				return err
			}
			defer dst.Free()

			log.Printf("start-recovery: bucket: %s, %s, index: %d, new-dst-groups: %v\n",
					ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, k.dst)

			dst.keys = append(dst.keys, k.resp.Key)
			//log.Printf("start-recovery: %s -> %v\n", k.resp.Key.String(), dst.groups)
			dest = append(dest, dst)
		}
	}

	log.Printf("start-recovery: bucket: %s, %s, index: %d, keys: %d: starting recovery\n",
		ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, len(ctl.rkeys))

	ctl.rkeys = ctl.rkeys[0:0]

	if true {
		var wait sync.WaitGroup
		for _, dst := range dest {
			s, err := ctl.gi.edge.DataSession([]uint32{ctl.gi.group_id})
			if err != nil {
				log.Printf("start-recovery: bucket: %s, %s, index: %d, dst-groups: %v: could not create data session: %v\n",
						ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, dst.groups, err)
				return err
			}
			defer s.Delete()


			dst.ssend, err = s.ServerSend(dst.keys, 0, dst.groups)
			if err != nil {
				log.Printf("start-recovery: bucket: %s, %s, index: %d, dst-groups: %v: server-send failed: %v\n",
						ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, dst.groups, err)
				return err
			}

			wait.Add(1)
			go func(dst *destination) {
				defer wait.Done()
				good, bad, err := dst.ReadServerSendResults()

				log.Printf("start-recovery: bucket: %s, %s, index: %d, dst-groups: %v, recovered: %d, errors: %d, total: %d, last error: %v\n",
						ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, dst.groups, good, bad, len(dst.keys), err)
				atomic.AddUint64(&ctl.good, good)
				atomic.AddUint64(&ctl.bad, bad)

				if err != nil {
					log.Fatalf("start-recovery: bucket: %s, could not read server-send results: %v\n", ctl.gi.bucket.Name, err)
				}
			}(dst)
		}
		wait.Wait()
	} else {
		for _, dst := range dest {
			for i, key := range dst.keys {
				f := elliptics.DnetIteratorResponse {
					Key:		key,
					IteratedKeys:	uint64(i),
					TotalKeys:	uint64(len(dst.keys)),
				}

				dst.failed = append(dst.failed, f)
			}
		}
	}

	log.Printf("start-recovery: bucket: %s, %s, index: %d, good: %d, bad: %d, keys: %d/%d: recovery completed\n",
			ctl.gi.bucket.Name, ctl.ab.String(), ctl.index, ctl.good, ctl.bad, ctl.good+ctl.bad, ctl.keys_to_recover)

	ctl.Fixup(dest)
	for _, dst := range dest {
		for _, fail := range dst.really_failed {
			log.Printf("start-recovery: bucket: %s, %s, index: %d, failed key: %s\n",
					ctl.gi.bucket.Name, ctl.ab.String(), ctl.index,	fail.Key.String())
		}
	}


	return nil
}

type GroupIteratorCtl struct {
	bucket		*bucket.Bucket

	iterators	map[int]*IteratorCtl
	empty		bool

	last_popped_response	*elliptics.DnetIteratorResponse
	prev			*elliptics.DnetIteratorResponse

	edge		*EdgeCtl

	s		*elliptics.Session
	sg		*elliptics.StatGroup

	group_id	uint32
	tmp_dir		string

	wait		sync.WaitGroup
}

func (e *EdgeCtl) DataSession(groups []uint32) (s *elliptics.Session, err error) {
	s, err = elliptics.NewSession(e.Ell.Node)
	if err != nil {
		return nil, err
	}

	s.SetTimeout(60)
	s.SetGroups(groups)

	return s, nil
}

func (e *EdgeCtl) NewGroupIteratorCtl(b *bucket.Bucket, tmp_dir string, group_id uint32, sg *elliptics.StatGroup) (gi *GroupIteratorCtl, err error) {
	gi = &GroupIteratorCtl {
		bucket:		b,
		iterators:	make(map[int]*IteratorCtl),
		empty:		true,
		sg:		sg,
		group_id:	group_id,
		tmp_dir:	path.Join(tmp_dir, strconv.FormatUint(uint64(group_id), 10)),
		edge:		e,
	}

	err = os.MkdirAll(gi.tmp_dir, os.ModeDir | 0755)
	if err != nil {
		log.Printf("new-group-iterator-ctl: could not create dir '%s': %v\n", gi.tmp_dir, err)
		return nil, err
	}

	gi.s, err = e.DataSession([]uint32{group_id})
	if err != nil {
		log.Printf("new-group-iterator-ctl: could not create new session: %v\n", err)
		return nil, err
	}

	return gi, nil
}

func (gi *GroupIteratorCtl) RunIterator() (err error) {
	iterator_idx := 0
	for ab, sb := range gi.sg.Ab {
		if len(sb.ID) == 0 {
			log.Printf("iterator-start: bucket: %s, %s: there are no IDs\n", gi.bucket.Name, ab.String())
			continue
		}

		ctl, err := gi.NewIteratorCtl(&ab, iterator_idx)
		if err != nil {
			log.Printf("iterator-start: bucket: %s, %s: could not create iterator controller for %s: %v\n",
				gi.bucket.Name, ab.String(), err)
			continue
		}

		ranges := make([]elliptics.DnetIteratorRange, 0)
		itype := elliptics.DNET_ITYPE_NETWORK
		iflags := elliptics.DNET_IFLAGS_NO_META
		id := sb.ID[0]

		ctl.in = gi.s.IteratorStart(&id, ranges, itype, iflags)

		gi.iterators[iterator_idx] = ctl
		gi.empty = false
		iterator_idx++

		gi.wait.Add(1)
		go func(ctl *IteratorCtl) {
			defer gi.wait.Done()

			err = ctl.ReadIteratorResponse()
			if err != nil {
				ctl.err = err
				gi.s.IteratorCancel(&id, ctl.iter_id)
				return
			}
		}(ctl)
	}

	if len(gi.iterators) == 0 {
		err = fmt.Errorf("iterator-start: bucket: %s: could not start any iterator from %d backends", gi.bucket.Name, len(gi.sg.Ab))
		return err
	}

	return nil
}

func (gi *GroupIteratorCtl) PopResponseGroupNoCheck() (min *elliptics.DnetIteratorResponse, err error) {
	if gi.empty {
		return nil, fmt.Errorf("Empty")
	}

	min_idx := -1
	var min_ctl *IteratorCtl

	if gi.last_popped_response != nil {
		min = gi.last_popped_response
		gi.last_popped_response = nil
		return min, nil
	}

	for _, ctl := range gi.iterators {
		if ctl.empty {
			continue
		}

		resp, idx, err := ctl.PopResponseIterator()
		if err != nil {
			ctl.empty = true
			log.Printf("recovery: bucket: %s, %s: failed to pop iterator response: %v\n", ctl.gi.bucket.Name, ctl.ab.String(), err)
			continue
		}

		if min_idx == -1 {
			min_ctl = ctl
			min = resp
			min_idx = idx
			continue
		}

		if KeyLess(resp, min) {
			min_ctl.PushResponse(min, min_idx)

			min_ctl = ctl
			min = resp
			min_idx = idx
		} else {
			min_ctl.PushResponse(resp, idx)
		}
	}

	if min_idx == -1 {
		gi.empty = true
		return nil, fmt.Errorf("Empty")
	}

	//log.Printf("pop-response-group: group: %d, key: %s\n", gi.group_id, (&min.Key).String())
	return min, nil
}

func (gi *GroupIteratorCtl) PopResponseGroup() (*elliptics.DnetIteratorResponse, error) {
	if gi.empty {
		return nil, fmt.Errorf("Empty")
	}

	for {
		cur, err := gi.PopResponseGroupNoCheck()
		if err != nil {
			return nil, err
		}

		if gi.prev == nil || !KeyEqual(cur, gi.prev) {
			gi.prev = cur
			//log.Printf("pop-response-group: group: %d, key: %s\n", gi.group_id, cur.Key.String())
			return cur, nil
		}
	}

	return nil, fmt.Errorf("impossible error")
}

func (gi *GroupIteratorCtl) PushResponseGroup(min *elliptics.DnetIteratorResponse) {
	gi.last_popped_response = min
	gi.prev = nil
}

func (e *EdgeCtl) LookupInfo(gis []*GroupIteratorCtl, merge_groups[]*elliptics.DnetIteratorResponse) (err error) {
	groups := make([]uint32, 0, len(merge_groups))
	var rr *elliptics.DnetIteratorResponse

	for idx, resp := range merge_groups {
		if resp != nil {
			groups = append(groups, gis[idx].group_id)
			rr = resp
		}
	}

	if rr == nil {
		log.Printf("lookup-info: all responses are nil")
		return fmt.Errorf("lookup-info: all responses are nil")
	}

	s, err := e.DataSession(groups)
	if err != nil {
		log.Printf("lookup-info: could not create data session for groups: %v, error: %v\n", groups, err)
		return err
	}
	defer s.Delete()

	for l := range s.ParallelLookupID(&rr.Key) {
		if l.Error() != nil {
			continue
		}

		// set the timestamp from lookup response
		group_id := l.Cmd().ID.Group
		for idx, gi := range gis {
			if gi.group_id == group_id {
				if merge_groups[idx] != nil {
					merge_groups[idx].Timestamp = l.Info().Mtime
					log.Printf("lookup-info: bucket: %s, key: %s: group: %d, size: %d, time: %s\n",
						gi.bucket.Name, rr.Key.String(), group_id, l.Info().Size, l.Info().Mtime.String())
					break
				}
			}
		}
	}

	return nil
}

func (e *EdgeCtl) Merge(gis []*GroupIteratorCtl) (err error) {
	merge_groups := make([]*elliptics.DnetIteratorResponse, len(gis), len(gis))

	for {
		for idx, _ := range merge_groups {
			merge_groups[idx] = nil
		}

		var min *elliptics.DnetIteratorResponse = nil
		min_idx := -1

		re := RecoveryEntry {
			dst:		make([]uint32, 0),
		}

		for idx, gi := range gis {
			mg, err := gi.PopResponseGroup()
			if err != nil {
				continue
			}

			if min_idx == -1 || min == nil {
				min = mg
				min_idx = idx

				merge_groups[min_idx] = mg
				continue
			}

			if KeyLess(mg, min) {
				gis[min_idx].PushResponseGroup(min)

				// clear all previously seen groups, keys there are all equal to @min or nil, which means are all higher than @mg
				for prev_idx := 0; prev_idx < idx; prev_idx++ {
					merge_groups[prev_idx] = nil
				}

				min = mg
				min_idx = idx
			} else if KeyEqual(mg, min) {
				merge_groups[min_idx] = mg
			} else {
				// this key is greater than @min, push it back into iterator
				gi.PushResponseGroup(mg)
			}
		}

		// we haven't found anything to recover, exit
		if min == nil {
			break
		}

		want_timestamp_sort := false

		// if we have timestamps, always perform timestamp-based search for 'source' recovery group
		if min.Timestamp == time.Unix(0, 0) {
			// run over groups we have found, if particular group is nil, we will recover key into it
			// if particular group is NOT nil, check its key's size (and timestamp if size differs)
			// if size differs, request timestamps from every group and run timestamp-based search for new source group
			for idx, mg := range merge_groups {
				if mg == nil {
					// there is no key in this group, recover key there
					re.dst = append(re.dst, gis[idx].group_id)
					continue
				}

				if mg == min {
					continue
				}

				// there is a key in given group, but its size differs from the size in @min group
				// if size differs and we do not have timestamp, lookup every key and set timestamps for all of them
				if min.Size != mg.Size && (min.Timestamp == time.Unix(0, 0) || mg.Timestamp == time.Unix(0, 0)) {
					e.LookupInfo(gis, merge_groups)

					log.Printf("merge: bucket: %s, key: %s: size mismatch: " +
						"min-size: %d, pretender-size: %d, timestamps: min: %s, pretender: %s\n",
						gis[idx].bucket.Name, min.Key.String(),
						min.Size, mg.Size, min.Timestamp.String(), mg.Timestamp.String())

					want_timestamp_sort = true
					break
				}

				// skip recovering key into @mg group *only* if its timestamp and size
				// equal to the @min timestamp (the newest key) and size
				if min.Timestamp == mg.Timestamp && min.Size == mg.Size {
					continue
				}

				// should not be reached
				re.dst = append(re.dst, gis[idx].group_id)
			}
		} else {
			want_timestamp_sort = true
		}

		if want_timestamp_sort {
			// clear dst array
			if len(re.dst) != 0 {
				re.dst = re.dst[:0]
			}

			// search for source group for timestamp based recovery
			for idx, mg := range merge_groups {
				if mg == nil || mg == min {
					continue
				}

				// select this new key as @min ('source' for recovery) if its timestamp is newer than current @min
				if mg.Timestamp.After(min.Timestamp) {
					min = mg
					min_idx = idx
				}
			}

			// @min contains the 'source' group for recovery, put everything else into destination array
			for idx, mg := range merge_groups {
				if mg == nil {
					// there is no key in this group, recovery it there
					re.dst = append(re.dst, gis[idx].group_id)
					continue
				}

				if mg == min {
					continue
				}

				// skip recovering key into @mg group *only* if its timestamp and size
				// equal to the @min timestamp (the newest key) and size
				if min.Timestamp == mg.Timestamp && min.Size == mg.Size {
					continue
				}

				// we have a key, but its timestamp is older
				re.dst = append(re.dst, gis[idx].group_id)
			}
		}

		// all replicas are alive
		if len(re.dst) == 0 {
			continue
		}

		re.resp = min

		gi := gis[min_idx]
		if gi == nil {
			err = fmt.Errorf("merge: bucket: %s, key: %s, min_idx: %d, gis: %v: gi is nil",
				gi.bucket.Name, re.resp.Key.String(), min_idx, gis)
			log.Printf("%s\n", err)
			return err
		}
		ctl := gi.iterators[int(min.ID)]
		if ctl == nil {
			err = fmt.Errorf("merge: bucket: %s, key: %s, group: %d, id: %d, iterators: %v: ctl is nil",
				gi.bucket.Name, re.resp.Key.String(), gi.group_id, min.ID, gi.iterators)
			log.Printf("%s\n", err)
			return err
		}

		ctl.rkeys = append(ctl.rkeys, &re)
		ctl.keys_to_recover += 1

		log.Printf("merge: bucket: %s, key: %s, group: %d, %s -> groups: %v\n",
			gi.bucket.Name, re.resp.Key.String(), gi.group_id, ctl.ab.String(), re.dst)

		if len(ctl.rkeys) == 1024 {
			err = ctl.StartRecovery()
			if err != nil {
				log.Printf("merge: bucket: %s, %s, group: %d, dst-groups: %v: recovery failed: %v\n",
					gi.bucket.Name, ctl.ab.String(), gi.group_id, re.dst, err)
				return err
			}
		}
	}

	for _, gi := range gis {
		for _, ctl := range gi.iterators {
			if len(ctl.rkeys) != 0 {
				err = ctl.StartRecovery()
				if err != nil {
					log.Printf("merge: bucket: %s, %s, group: %d: recovery failed: %v\n",
						gi.bucket.Name, ctl.ab.String(), gi.group_id, err)
					return err
				}
			}
		}
	}

	log.Printf("bucket: %s: recovery completed\n", gis[0].bucket.Name)
	return nil
}

func (e *EdgeCtl) BucketRecovery(b *bucket.Bucket) (error) {
	tmp := path.Join(e.TmpPath, fmt.Sprintf("recovery.%d.%s", strconv.Itoa(os.Getpid()), b.Name))

	os.RemoveAll(tmp)
	err := os.MkdirAll(tmp, os.ModeDir | 0755)
	if err != nil {
		log.Printf("bucket-recovery: could not create temporal directory '%s': %v\n", tmp, err)
		return err
	}
	defer os.RemoveAll(tmp)

	gis := make([]*GroupIteratorCtl, 0)
	for group_id, sg := range b.Group {
		gi, err := e.NewGroupIteratorCtl(b, tmp, group_id, sg)
		if err != nil {
			log.Printf("bucket-recovery: bucket: %s: could not create iterator control structure for group %d: %v\n", b.Name, group_id, err)
			return err
		}

		err = gi.RunIterator()
		if err != nil {
			log.Printf("bucket-recovery: bucket: %s: could not start copy iterator in group %d: %v\n", b.Name, group_id, err)
			return err
		}

		gis = append(gis, gi)
	}

	if len(gis) <= 1 {
		err = fmt.Errorf("bucket-recovery: bucket: %s: required at least 2 group iterators, but we have %d, exiting\n", b.Name, len(gis))
		log.Printf("%v\n", err)
		return err
	}

	var recovery_failed error
	for _, gi := range gis {
		gi.wait.Wait()

		for _, ctl := range gi.iterators {
			if ctl.err != nil {
				log.Printf("bucket-recovery: bucket: %s: iterator %s has failed: %v\n", b.Name, ctl.ab.String(), ctl.err)
				recovery_failed = ctl.err
			}
		}
	}

	if recovery_failed != nil {
		return recovery_failed
	}

	err = e.Merge(gis)
	if err != nil {
		return err
	}

	return nil
}
