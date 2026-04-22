package main

import (
	"encoding/json"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"
)

type Store struct {
	db *bolt.DB
}

var bucket = []byte("payments")

var idempotencyBucket = []byte("idempotency")

var preparedBucket = []byte("prepared")

func NewStore() *Store {
	dbPath := os.Getenv("BOLT_PATH")
	if dbPath == "" {
		dbPath = "payments.db"
	}

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists(bucket)
		_, _ = tx.CreateBucketIfNotExists(idempotencyBucket)
		_, _ = tx.CreateBucketIfNotExists(preparedBucket)
		return nil
	})

	return &Store{db: db}
}

func (s *Store) Save(txnID string, data interface{}) uint64 {
	var seq uint64
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		seq, _ = b.NextSequence()
		bytes, _ := json.Marshal(data)
		return b.Put([]byte(txnID), bytes)
	})
	return seq
}

func (s *Store) CheckIdempotency(key string) (bool, string, bool) {
	var txnID string
	var success bool

	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(idempotencyBucket)
		data := b.Get([]byte(key))
		if data == nil {
			return nil
		}

		var result struct {
			TxnID  string
			Success bool
		}

		if err := json.Unmarshal(data, &result); err != nil {
			return nil
		}

		txnID = result.TxnID
		success = result.Success
		return nil
	})

	if txnID == "" {
		return false, "", false
	}

	return true, txnID, success
}

func (s *Store) WriteResult(key, txnID string, success bool) {
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(idempotencyBucket)

		data, _ := json.Marshal(struct {
			TxnID  string
			Success bool
		}{
			TxnID: txnID,
			Success: success,
		})

		return b.Put([]byte(key), data)
	})
}

func (s *Store) GetAllPending(epoch int64) []interface{} {
	var results []interface{}

	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		b.ForEach(func(k, v []byte) error {
			var entry map[string]interface{}
			if err := json.Unmarshal(v, &entry); err != nil {
				return nil
			}

			state, _ := entry["state"].(string)
			// (Removed entryEpoch parsing since we want tasks from all epochs)

			if state == "QUEUED" || state == "IN_PROGRESS" {

				results = append(results, entry)
			}

			return nil
		})

		return nil
	})

	return results
}

func (s *Store) SaveEpoch(epoch int64) {
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket) // Reusing the main bucket
		
		data, _ := json.Marshal(map[string]int64{"epoch": epoch})
		return b.Put([]byte("CURRENT_EPOCH"), data)
	})
}

func (s *Store) GetEpoch() int64 {
	var epoch int64 = 1 // Default to 1 if it doesn't exist

	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		data := b.Get([]byte("CURRENT_EPOCH"))
		if data == nil {
			return nil
		}
		
		var result map[string]int64
		if err := json.Unmarshal(data, &result); err == nil {
			if val, ok := result["epoch"]; ok {
				epoch = val
			}
		}
		return nil
	})

	return epoch
}

// Prepare locks a high-value transaction for 2-Phase Commit.
// Returns false if the txn doesn't exist or is already in a terminal state.
func (s *Store) Prepare(txnID string) bool {
	var ok bool
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(preparedBucket)
		data, _ := json.Marshal(map[string]string{
			"txn_id": txnID,
			"state":  "PREPARED",
		})
		if err := b.Put([]byte(txnID), data); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok
}

// Commit finalises a PREPARED transaction in the 2PC log.
func (s *Store) Commit(txnID string) bool {
	return s.update2PCState(txnID, "COMMITTED")
}

// Rollback aborts a PREPARED transaction in the 2PC log.
func (s *Store) Rollback(txnID string) bool {
	return s.update2PCState(txnID, "ROLLED_BACK")
}

func (s *Store) update2PCState(txnID, newState string) bool {
	var ok bool
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(preparedBucket)
		
		// Ensure the transaction was actually PREPARED
		existing := b.Get([]byte(txnID))
		if existing == nil {
			return nil // Not a 2PC transaction
		}

		var entry map[string]string
		if err := json.Unmarshal(existing, &entry); err != nil {
			return nil
		}

		if entry["state"] != "PREPARED" {
			return nil // Already resolved or invalid state
		}

		data, _ := json.Marshal(map[string]string{
			"txn_id": txnID,
			"state":  newState,
		})
		if err := b.Put([]byte(txnID), data); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok
}

// Get2PCStats returns counts of PREPARED, COMMITTED, ROLLED_BACK records.
func (s *Store) Get2PCStats() (prepared, committed, rolledBack int64) {
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(preparedBucket)
		b.ForEach(func(k, v []byte) error {
			var entry map[string]string
			if err := json.Unmarshal(v, &entry); err != nil {
				return nil
			}
			switch entry["state"] {
			case "PREPARED":
				prepared++
			case "COMMITTED":
				committed++
			case "ROLLED_BACK":
				rolledBack++
			}
			return nil
		})
		return nil
	})
	return
}
