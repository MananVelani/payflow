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

func NewStore() *Store {
	db, err := bolt.Open("payments.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists(bucket)
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