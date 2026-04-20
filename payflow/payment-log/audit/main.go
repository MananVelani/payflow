package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"
)

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func main() {
	log.Println("Starting PayFlow C4 Audit CLI...")

	dbPath := os.Getenv("BOLT_PATH")
	if dbPath == "" {
		dbPath = "payments.db"
	}
	auditDBPath := "/tmp/audit.db"

	// 1. Copy the database to a temporary location to 100% guarantee we don't
	// interrupt or lock out the live Payment Log server while it's processing real payments!
	if err := copyFile(dbPath, auditDBPath); err != nil {
		log.Fatalf("FAILED: Could not create audit snapshot of database: %v", err)
	}

	// 2. Open the snapshot in read-only mode
	db, err := bolt.Open(auditDBPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatalf("FAILED: Could not open database snapshot: %v", err)
	}
	defer db.Close()

	auditResults := map[string]interface{}{}
	var payments []interface{}
	var idempotency []interface{}

	// 3. Scan the buckets
	err = db.View(func(tx *bolt.Tx) error {
		// --- Payments Bucket ---
		bucket := tx.Bucket([]byte("payments"))
		if bucket != nil {
			bucket.ForEach(func(k, v []byte) error {
				var entry interface{}
				if err := json.Unmarshal(v, &entry); err == nil {
					payments = append(payments, map[string]interface{}{
						"key":  string(k),
						"data": entry,
					})
				}
				return nil
			})
		}

		// --- Idempotency Bucket ---
		idemBucket := tx.Bucket([]byte("idempotency"))
		if idemBucket != nil {
			idemBucket.ForEach(func(k, v []byte) error {
				var entry interface{}
				if err := json.Unmarshal(v, &entry); err == nil {
					idempotency = append(idempotency, map[string]interface{}{
						"key":  string(k),
						"data": entry,
					})
				}
				return nil
			})
		}
		return nil
	})

	if err != nil {
		log.Fatalf("FAILED: Error reading database: %v", err)
	}

	auditResults["total_payments"] = len(payments)
	auditResults["payments"] = payments
	auditResults["total_idempotency_keys"] = len(idempotency)
	auditResults["idempotency"] = idempotency

	// 4. Format Output as beautiful JSON
	outputBytes, err := json.MarshalIndent(auditResults, "", "  ")
	if err != nil {
		log.Fatalf("FAILED: Could not format JSON: %v", err)
	}

	// 5. Output to terminal
	fmt.Println("\n--- AUDIT REPORT BEGIN ---")
	fmt.Println(string(outputBytes))
	fmt.Println("--- AUDIT REPORT END ---\n")

	// 6. Write physically to file mapped to the Docker volume so Member 5 can access it if needed
	reportPath := "/data/audit_report.json"
	if err := os.WriteFile(reportPath, outputBytes, 0644); err != nil {
		log.Printf("WARNING: Failed to write report to %s: %v", reportPath, err)
	} else {
		log.Printf("SUCCESS: Audit report saved permanently to %s", reportPath)
	}
}
