#!/bin/bash

# Firebird Load Generator Examples
# This script demonstrates various ways to use the fb-loadgen tool

echo "=== Firebird Load Generator Examples ==="
echo

# Example 1: Basic write-heavy test
echo "1. Basic write-heavy test (30s warmup, 120s main, 20s cooldown)"
echo "./fb-loadgen --profile write-heavy"
echo

# Example 2: Read-heavy test with custom connection settings
echo "2. Read-heavy test with custom connection settings"
echo "./fb-loadgen --profile read-heavy --conn \"localhost/3055:./EMPLOYEE.FDB\" --user SYSDBA --pass masterkey --conn-init 5 --conn-peak 50"
echo

# Example 3: Spike testing
echo "3. Spike testing with 5 cycles of 15 seconds each"
echo "./fb-loadgen --profile spike --spike-cycles 5 --spike-hold 15 --main 120"
echo

# Example 4: Short test for development
echo "4. Short test for development/debugging"
echo "./fb-loadgen --profile write-heavy --warmup 5 --main 10 --cooldown 5 --conn-init 1 --conn-peak 3 --think-ms 0"
echo

# Example 5: Output to CSV
echo "5. Output results to CSV file"
echo "./fb-loadgen --profile write-heavy --csv my_test_results.csv --report-every 10"
echo

# Example 6: Dry-run mode
echo "6. Dry-run mode to test configuration"
echo "./fb-loadgen --profile write-heavy --dry-run"
echo

# Example 7: Custom timing
echo "7. Custom timing for specific testing scenarios"
echo "./fb-loadgen --profile read-heavy --warmup 60 --main 300 --cooldown 30 --conn-init 10 --conn-peak 100"
echo

echo "=== Common Options ==="
echo "--profile: write-heavy | read-heavy | spike (required)"
echo "--conn: Database connection string"
echo "--user: Database username"
echo "--pass: Database password"
echo "--conn-init: Initial number of connections"
echo "--conn-peak: Peak number of connections"
echo "--warmup: Ramp-up period in seconds"
echo "--main: Main test period in seconds"
echo "--cooldown: Ramp-down period in seconds"
echo "--csv: Output CSV file path"
echo "--report-every: Console report interval in seconds"
echo "--think-ms: Worker think time between operations"
echo "--tx-timeout: Statement timeout in seconds"
echo "--dry-run: Test configuration without running load"
echo

echo "=== Help ==="
echo "For complete help: ./fb-loadgen --help"