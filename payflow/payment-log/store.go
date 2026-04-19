package main

import (
	"encoding/json"
	"log"

	bolt "go.etcd.io/bbolt"
)

type Store struct {
	db *bolt.DB
}

var bucket = []byte("payments")

var idempotencyBucket = []byte("idempotency")

func NewStore() *Store {
	db, err := bolt.Open("payments.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists(bucket)
		_, _ = tx.CreateBucketIfNotExists(idempotencyBucket)
		return nil
	})

	return &Store{db: db}
}

func (s *Store) Save(txnID string, data interface{}) {
	s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		bytes, _ := json.Marshal(data)
		return b.Put([]byte(txnID), bytes)
	})
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
			entryEpoch, _ := entry["epoch"].(float64)

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
