package db_test

import (
	"os"
	"path/filepath"
	"testing"

	"fb-loadgen/config"
	"fb-loadgen/db"
)

func TestValidateSchemaGateAgainstSample(t *testing.T) {
	dsn := os.Getenv("FB_TEST_DSN")
	if dsn == "" {
		abs, err := filepath.Abs("..\\EMPLOYEE.FDB")
		if err != nil {
			t.Skip(err)
		}
		if _, err := os.Stat(abs); err != nil {
			t.Skip("no EMPLOYEE.FDB and FB_TEST_DSN unset")
		}
		dsn = "localhost/3050:" + abs
	}
	cfg := &config.Config{
		DSN:       dsn,
		User:      getenv("FB_TEST_USER", "SYSDBA"),
		Pass:      getenv("FB_TEST_PASS", "masterkey"),
		TxTimeout: 10,
	}
	f := db.NewConnectionFactory(cfg)
	if err := f.ValidateSchemaGate(); err != nil {
		t.Fatalf("ValidateSchemaGate: %v", err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
