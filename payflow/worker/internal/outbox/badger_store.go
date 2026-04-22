package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// BadgerStore implements the Store interface using an embedded BadgerDB.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore initializes a BadgerDB at the given path.
func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	// Suppress badger logging unless it's an error
	opts.Logger = nil 
	
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badger open: %w", err)
	}
	
	return &BadgerStore{db: db}, nil
}

func (s *BadgerStore) Append(ctx context.Context, entry Entry) error {
	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}
	
	key := []byte(fmt.Sprintf("outbox:%019d:%s", entry.CreatedAt.UnixNano(), entry.ID))
	val, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, val).WithTTL(24 * time.Hour)
		return txn.SetEntry(e)
	})
}

func (s *BadgerStore) Pending(ctx context.Context) ([]Entry, error) {
	var entries []Entry
	
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 100
		
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("outbox:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				var entry Entry
				if err := json.Unmarshal(v, &entry); err != nil {
					return err
				}
				// The key in Badger is the true ID for Acknowledgment
				entry.ID = string(item.Key())
				entries = append(entries, entry)
				return nil
			})
			if err != nil {
				return err
			}
			
			if len(entries) >= 100 {
				break
			}
		}
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("badger view: %w", err)
	}
	return entries, nil
}

func (s *BadgerStore) Ack(ctx context.Context, id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(id))
	})
}

// SetLease creates or updates a task lease with a TTL.
func (s *BadgerStore) SetLease(ctx context.Context, taskID string, ttl time.Duration) error {
	key := []byte(fmt.Sprintf("task:%s", taskID))
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, []byte("in-flight")).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// DeleteLease removes a task lease.
func (s *BadgerStore) DeleteLease(ctx context.Context, taskID string) error {
	key := []byte(fmt.Sprintf("task:%s", taskID))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// ListLeases returns all active task IDs from Badger.
func (s *BadgerStore) ListLeases(ctx context.Context) ([]string, error) {
	var taskIDs []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		
		prefix := []byte("task:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// Extract taskID from "task:<id>"
			taskIDs = append(taskIDs, string(key[len(prefix):]))
		}
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("badger list leases: %w", err)
	}
	return taskIDs, nil
}

func (s *BadgerStore) Close() error {
	return s.db.Close()
}
