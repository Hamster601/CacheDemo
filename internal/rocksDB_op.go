package internal

// #include <stdlib.h>
// #include "rocksdb/c.h"
// #cgo CFLAGS: -I${SRCDIR}/../../../rocksdb/include
// #cgo LDFLAGS: -L${SRCDIR}/../../../rocksdb -lrocksdb -lz -lpthread -lsnappy -lstdc++ -lm -O3
import "C"
import (
	"time"
	"unsafe"
)
//批量写入磁盘的键值对数
const BATCH_SIZE = 100

func flush_batch(db *C.rocksdb_t, b *C.rocksdb_writebatch_t, o *C.rocksdb_writeoptions_t) {
	var e *C.char
	C.rocksdb_write(db, o, b, &e)
	if e != nil {
		panic(C.GoString(e))
	}
	C.rocksdb_writebatch_clear(b)
}

func write_func(db *C.rocksdb_t, c chan *pair, o *C.rocksdb_writeoptions_t) {
	count := 0
	t := time.NewTimer(time.Second)
	//批量写入rocks DB的结构体指针
	b := C.rocksdb_writebatch_create()
	for {
		select {
		//读取键值对
		case p := <-c:
			count++
			key := C.CString(p.k)
			value := C.CBytes(p.v)
			C.rocksdb_writebatch_put(b, key, C.size_t(len(p.k)), (*C.char)(value), C.size_t(len(p.v)))
			//释放指针
			C.free(unsafe.Pointer(key))
			C.free(value)
			//达到100时写入
			if count == BATCH_SIZE {
				flush_batch(db, b, o)
				count = 0
			}
			if !t.Stop() {
				<-t.C
			}
			//重置时间
			t.Reset(time.Second)
		//超时控制，未满100直接写入rocksDB,并将count置0
		case <-t.C:
			if count != 0 {
				flush_batch(db, b, o)
				count = 0
			}
			//重置时间
			t.Reset(time.Second)
		}
	}
}

func (c *rocksdbCache) Set(key string, value []byte) error {
	c.ch <- &pair{key, value}
	return nil
}

// #include "rocksdb/c.h"
// #cgo CFLAGS: -I${SRCDIR}/../../../rocksdb/include
// #cgo LDFLAGS: -L${SRCDIR}/../../../rocksdb -lrocksdb -lz -lpthread -lsnappy -lstdc++ -lm -O3
import "C"
import "unsafe"

type rocksdbScanner struct {
	i           *C.rocksdb_iterator_t
	initialized bool
}

func (s *rocksdbScanner) Close() {
	C.rocksdb_iter_destroy(s.i)
}

func (s *rocksdbScanner) Scan() bool {
	if !s.initialized {
		C.rocksdb_iter_seek_to_first(s.i)
		s.initialized = true
	} else {
		C.rocksdb_iter_next(s.i)
	}
	return C.rocksdb_iter_valid(s.i) != 0
}

func (s *rocksdbScanner) Key() string {
	var length C.size_t
	k := C.rocksdb_iter_key(s.i, &length)
	return C.GoString(k)
}

func (s *rocksdbScanner) Value() []byte {
	var length C.size_t
	v := C.rocksdb_iter_value(s.i, &length)
	return C.GoBytes(unsafe.Pointer(v), C.int(length))
}

func (c *rocksdbCache) NewScanner() Scanner {
	return &rocksdbScanner{C.rocksdb_create_iterator(c.db, c.ro), false}
}

// #include <stdlib.h>
// #include "rocksdb/c.h"
// #cgo CFLAGS: -I${SRCDIR}/../../../rocksdb/include
// #cgo LDFLAGS: -L${SRCDIR}/../../../rocksdb -lrocksdb -lz -lpthread -lsnappy -lstdc++ -lm -O3
import "C"
import (
	"regexp"
	"strconv"
	"unsafe"
)

func (c *rocksdbCache) GetStat() Stat {
	k := C.CString("rocksdb.aggregated-table-properties")
	defer C.free(unsafe.Pointer(k))
	v := C.rocksdb_property_value(c.db, k)
	defer C.free(unsafe.Pointer(v))
	p := C.GoString(v)
	r := regexp.MustCompile(`([^;]+)=([^;]+);`)
	s := Stat{}
	for _, submatches := range r.FindAllStringSubmatch(p, -1) {
		if submatches[1] == " # entries" {
			s.Count, _ = strconv.ParseInt(submatches[2], 10, 64)
		} else if submatches[1] == " raw key size" {
			s.KeySize, _ = strconv.ParseInt(submatches[2], 10, 64)
		} else if submatches[1] == " raw value size" {
			s.ValueSize, _ = strconv.ParseInt(submatches[2], 10, 64)
		}
	}
	return s
}

// #include <stdlib.h>
// #include "rocksdb/c.h"
// #cgo CFLAGS: -I${SRCDIR}/../../../rocksdb/include
// #cgo LDFLAGS: -L${SRCDIR}/../../../rocksdb -lrocksdb -lz -lpthread -lsnappy -lstdc++ -lm -O3
import "C"
import (
	"errors"
	"unsafe"
)

func (c *rocksdbCache) Get(key string) ([]byte, error) {
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	var length C.size_t
	v := C.rocksdb_get(c.db, c.ro, k, C.size_t(len(key)), &length, &c.e)
	if c.e != nil {
		return nil, errors.New(C.GoString(c.e))
	}
	defer C.free(unsafe.Pointer(v))
	return C.GoBytes(unsafe.Pointer(v), C.int(length)), nil
}


// #include <stdlib.h>
// #include "rocksdb/c.h"
// #cgo CFLAGS: -I${SRCDIR}/../../../rocksdb/include
// #cgo LDFLAGS: -L${SRCDIR}/../../../rocksdb -lrocksdb -lz -lpthread -lsnappy -lstdc++ -lm -O3
import "C"
import (
	"errors"
	"unsafe"
)

func (c *rocksdbCache) Del(key string) error {
	k := C.CString(key)
	defer C.free(unsafe.Pointer(k))
	C.rocksdb_delete(c.db, c.wo, k, C.size_t(len(key)), &c.e)
	if c.e != nil {
		return errors.New(C.GoString(c.e))
	}
	return nil
}
