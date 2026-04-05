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

func(s *Store) CheckIdempotency(key string) (bool, string, bool) {
	var taxnID string
	var success bool
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(idempotencyBucket)
		data := b.Get([]byte(key))
		if data == nil {
			return nil;
		}
		var result struct {
			TxnID   string
			Success bool
		}
		json.Unmarshal(data, &result)
		taxnID = result.TxnID
		success = result.Success
		return nil
	})
	if taxnID == "" {
		return false, "", false
	}
	return true, taxnID, success
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
