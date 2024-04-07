package main

import (
    "io"
    "encoding/gob"
    "fmt"
    "log"
    "os"
    "sync"
    "time"
    "strings"
    "path"
)

// DB represents an embedded database.
type DB struct {
	mu           sync.RWMutex      // Mutex for thread-safe operations
	data         map[string][]byte // In-memory data store
	walLog       *os.File          // Write-Ahead Log file
	snapshotDir  string            // Directory to store database snapshots
	snapshotTTL  time.Duration     // Interval for saving snapshots
}

// Open opens a new database instance.
func Open(path string, snapshotTTL time.Duration) (*DB, error) {
	walLog, err := os.OpenFile(path+".wal", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	db := &DB{
		data:        make(map[string][]byte),
		walLog:      walLog,
		snapshotDir: path,
		snapshotTTL: snapshotTTL,
	}

	// Recover data from WAL log or snapshot
	err = db.recover()
	if err != nil {
		return nil, err
	}

	// Start snapshot routine
	go db.snapshotRoutine()

	return db, nil
}

// Get retrieves the value for the given key.
func (db *DB) Get(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data[key], nil
}

// Put inserts or updates the value for the given key.
func (db *DB) Put(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to WAL log
	enc := gob.NewEncoder(db.walLog)
	err := enc.Encode(&WalEntry{Op: "put", Key: key, Value: value})
	if err != nil {
		return err
	}

	// Update in-memory data
	db.data[key] = value
	return nil
}

// Delete removes the value for the given key.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to WAL log
	enc := gob.NewEncoder(db.walLog)
	err := enc.Encode(&WalEntry{Op: "delete", Key: key})
	if err != nil {
		return err
	}

	// Update in-memory data
	delete(db.data, key)
	return nil
}

// Update modifies the value for the given key using the provided function.
func (db *DB) Update(key string, updateFunc func([]byte) ([]byte, error)) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	value, ok := db.data[key]
	if !ok {
		return fmt.Errorf("key %s does not exist", key)
	}

	newValue, err := updateFunc(value)
	if err != nil {
		return err
	}

	// Write to WAL log
	enc := gob.NewEncoder(db.walLog)
	err = enc.Encode(&WalEntry{Op: "put", Key: key, Value: newValue})
	if err != nil {
		return err
	}

	// Update in-memory data
	db.data[key] = newValue
	return nil
}

// IterKeys returns a slice of all keys in the database.
func (db *DB) IterKeys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	keys := make([]string, 0, len(db.data))
	for k := range db.data {
		keys = append(keys, k)
	}
	return keys
}

// MultiGet retrieves the values for the given keys.
func (db *DB) MultiGet(keys []string) (map[string][]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		values[key] = db.data[key]
	}
	return values, nil
}

// Close closes the database and underlying resources.
func (db *DB) Close() error {
	return db.walLog.Close()
}

// RepairDB attempts to recover data from the WAL log in case of corruption.
func (db *DB) RepairDB() error {
	return db.recover()
}

// recover reads the WAL log and the latest snapshot to restore the in-memory data.
func (db *DB) recover() error {
	// Load data from the latest snapshot
	snapshot, err := db.loadLatestSnapshot()
	if err != nil {
		return err
	}
	db.data = snapshot

	// Apply changes from the WAL log
	_, err = db.walLog.Seek(0, 0)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(db.walLog)
	for {
		var entry WalEntry
		err := dec.Decode(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch entry.Op {
		case "put":
			db.data[entry.Key] = entry.Value
		case "delete":
			delete(db.data, entry.Key)
		}
	}

	return nil
}

// loadLatestSnapshot loads the latest database snapshot from disk.
func (db *DB) loadLatestSnapshot() (map[string][]byte, error) {
	files, err := os.ReadDir(db.snapshotDir)
	if err != nil {
		return nil, err
	}

	var latestSnapshot map[string][]byte
	var latestTime time.Time
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".snapshot") {
			continue
		}

        fileInfo, err := file.Info()
		if err != nil {
			log.Printf("Failed to get file info for %s: %v", file.Name(), err)
			continue
		}

		modTime := fileInfo.ModTime()
		if modTime.After(latestTime) {
			snapshot, err := db.loadSnapshot(file.Name())
			if err != nil {
				log.Printf("Failed to load snapshot %s: %v", file.Name(), err)
				continue
			}
			latestSnapshot = snapshot
			latestTime = modTime
		}
	}

	return latestSnapshot, nil
}

// loadSnapshot loads a database snapshot from disk.
func (db *DB) loadSnapshot(filename string) (map[string][]byte, error) {
	file, err := os.Open(path.Join(db.snapshotDir, filename))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var snapshot map[string][]byte
	dec := gob.NewDecoder(file)
	err = dec.Decode(&snapshot)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// snapshotRoutine periodically saves a snapshot of the in-memory data to disk.
func (db *DB) snapshotRoutine() {
	ticker := time.NewTicker(db.snapshotTTL)
	defer ticker.Stop()

	for {
		<-ticker.C
		db.saveSnapshot()
	}
}

// saveSnapshot saves a snapshot of the in-memory data to disk and clears the WAL file.
func (db *DB) saveSnapshot() {
	db.mu.RLock()
	defer db.mu.RUnlock()

	filename := fmt.Sprintf("%d.snapshot", time.Now().UnixNano())
	file, err := os.Create(path.Join(db.snapshotDir, filename))
	if err != nil {
		log.Printf("Failed to create snapshot file %s: %v", filename, err)
		return
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	err = enc.Encode(db.data)
	if err != nil {
		log.Printf("Failed to encode snapshot %s: %v", filename, err)
		return
	}

	// Clean up old snapshots while holding the read lock
	db.cleanupSnapshots()

	// Clear the WAL file
	db.mu.Lock()
	defer db.mu.Unlock()
	err = db.walLog.Truncate(0)
	if err != nil {
		log.Printf("Failed to truncate WAL file: %v", err)
		return
	}
	_, err = db.walLog.Seek(0, 0)
	if err != nil {
		log.Printf("Failed to seek to the beginning of WAL file: %v", err)
		return
	}
}

// cleanupSnapshots removes old snapshots, keeping only the latest one.
func (db *DB) cleanupSnapshots() {
    files, err := os.ReadDir(db.snapshotDir)
	if err != nil {
		log.Printf("Failed to read snapshot directory: %v", err)
		return
	}

	var latestModTime time.Time
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".snapshot") {
			continue
		}

		fileInfo, err := file.Info()
		if err != nil {
			log.Printf("Failed to get file info for %s: %v", file.Name(), err)
			continue
		}

		modTime := fileInfo.ModTime()
		if modTime.After(latestModTime) {
			latestModTime = modTime
		}
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".snapshot") {
			continue
		}

		fileInfo, err := file.Info()
		if err != nil {
			log.Printf("Failed to get file info for %s: %v", file.Name(), err)
			continue
		}

		modTime := fileInfo.ModTime()
		if modTime.Before(latestModTime) {
			os.Remove(path.Join(db.snapshotDir, file.Name()))
		}
	}
}

// WalEntry represents a log entry in the Write-Ahead Log.
type WalEntry struct {
	Op    string
	Key   string
	Value []byte
}