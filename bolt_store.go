package raftboltdb

import (
	"errors"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BoltStore provides access to BoltDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BoltStore struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// The path to the Bolt database file
	path string
}

// Options contains all the configuration used to open the BoltDB
type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
}

// readOnly returns true if the contained bolt options say to open
// the DB in readOnly mode [this can be useful to tools that want
// to examine the log]
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// NewBoltStore takes a file path and returns a connected Raft backend.
func NewBoltStore(path string) (*BoltStore, error) {
	return New(Options{Path: path})
}

// New uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
func New(options Options) (*BoltStore, error) {
	// Try to connect
	handle, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new store
	store := &BoltStore{
		conn: handle,
		path: options.Path,
	}

	// If the store was opened read-only, don't try and create buckets
	if !options.readOnly() {
		// Set up our buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

// initialize is used to set up all of the buckets.
func (b *BoltStore) initialize() error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create all the buckets
	if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (b *BoltStore) Close() error {
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return raft.ErrLogNotFound
	}
	return decodeMsgPack(val, log)
}

// StoreLog is used to store a single raft log
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(dbLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Set is used to set a key/value set outside of the raft log
func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *BoltStore) Sync() error {
	return b.conn.Sync()
}

type TxStats struct {
	// Page statistics.
	PageCount int `json:"page_count"` // number of page allocations
	PageAlloc int `json:"page_alloc"` // total bytes allocated

	// Cursor statistics.
	CursorCount int `json:"cursor_count"` // number of cursors created

	// Node statistics
	NodeCount int `json:"node_count"` // number of node allocations
	NodeDeref int `json:"node_deref"` // number of node dereferences

	// Rebalance statistics.
	Rebalance     int           `json:"rebalance"`      // number of node rebalances
	RebalanceTime time.Duration `json:"rebalance_time"` // total time spent rebalancing

	// Split/Spill statistics.
	Split     int           `json:"split"`      // number of nodes split
	Spill     int           `json:"spill"`      // number of nodes spilled
	SpillTime time.Duration `json:"spill_time"` // total time spent spilling

	// Write statistics.
	Write     int           `json:"write"`      // number of writes performed
	WriteTime time.Duration `json:"write_time"` // total time spent writing to disk
}

// Stats is the statistics for the BoltDB store.
type Stats struct {
	// Freelist stats
	FreePageN     int `json:"num_free_pages"`    // total number of free pages on the freelist
	PendingPageN  int `json:"num_pending_pages"` // total number of pending pages on the freelist
	FreeAlloc     int `json:"free_alloc"`        // total bytes allocated in free pages
	FreelistInuse int `json:"free_list_inuse"`   // total bytes used by the freelist

	// Transaction stats
	TxN     int `json:"num_tx_read"` // total number of started read transactions
	OpenTxN int `json:"num_tx_open"` // number of currently open read transactions

	TxStats TxStats `json:"tx_stats"` // global, ongoing stats.
}

// Stats returns the BoltStore statistics.
func (b *BoltStore) Stats() Stats {
	stats := b.conn.Stats()

	txStats := TxStats{
		PageCount:     stats.TxStats.PageCount,
		PageAlloc:     stats.TxStats.PageAlloc,
		CursorCount:   stats.TxStats.CursorCount,
		NodeCount:     stats.TxStats.NodeCount,
		NodeDeref:     stats.TxStats.NodeDeref,
		Rebalance:     stats.TxStats.Rebalance,
		RebalanceTime: stats.TxStats.RebalanceTime,
		Split:         stats.TxStats.Split,
		Spill:         stats.TxStats.Spill,
		SpillTime:     stats.TxStats.SpillTime,
		Write:         stats.TxStats.Write,
		WriteTime:     stats.TxStats.WriteTime,
	}
	return Stats{
		FreePageN:     stats.FreePageN,
		PendingPageN:  stats.PendingPageN,
		FreeAlloc:     stats.FreeAlloc,
		FreelistInuse: stats.FreelistInuse,
		TxN:           stats.TxN,
		OpenTxN:       stats.OpenTxN,
		TxStats:       txStats,
	}
}
