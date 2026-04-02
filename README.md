# Firebird Load Generator (fb-loadgen)

A high-performance Go-based load testing tool for Firebird databases, designed to simulate realistic workloads with configurable profiles and connection scaling.

## Features

- **Three Workload Profiles**: Write-heavy, read-heavy, and spike testing
- **Connection Scaling**: Gradual ramp-up and ramp-down with configurable rates
- **Realistic Operations**: Based on the EMPLOYEE sample database schema
- **Comprehensive Metrics**: Latency, throughput, and error tracking
- **Multiple Output Formats**: Text, JSON, and CSV reporting
- **Graceful Shutdown**: Clean termination with signal handling
- **Dry-run Mode**: Test configuration without executing load

## Quick Start

### Prerequisites

- Go 1.20+
- Firebird database server
- EMPLOYEE sample database

### Installation

```bash
go get github.com/your-org/fb-loadgen
cd $GOPATH/src/github.com/your-org/fb-loadgen
go build
```

### Basic Usage

```bash
# Run write-heavy profile with defaults (localhost:3055)
./fb-loadgen --profile write-heavy

# Custom connection settings
./fb-loadgen --profile read-heavy \
    --dsn "localhost/3055:./EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey

# Spike testing with custom timing
./fb-loadgen --profile spike \
    --warmup 10 \
    --main 60 \
    --cooldown 10 \
    --spike-cycles 2 \
    --spike-hold 5

# Output to CSV
./fb-loadgen --profile write-heavy \
    --csv results.csv \
    --report-every 10

# Dry-run mode to verify configuration
./fb-loadgen --profile write-heavy --dry-run
```

### Complete Example Workflow

```bash
# 1. Build the application
go build -o fb-loadgen.exe .

# 2. Test configuration without running load
./fb-loadgen --profile write-heavy \
    --dsn "localhost/3055:./EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey \
    --conn-init 5 \
    --conn-peak 20 \
    --dry-run

# 3. Run short warmup test
./fb-loadgen --profile write-heavy \
    --dsn "localhost/3055:./EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey \
    --warmup 10 \
    --main 30 \
    --cooldown 10 \
    --conn-init 5 \
    --conn-peak 10

# 4. Full production run with CSV output (all parameters)
./fb-loadgen \
    --dsn "localhost/3055:e:/Projects_2026/firebirdgotester/EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey \
    --profile write-heavy \
    --conn-init 2 \
    --conn-peak 20 \
    --warmup 30 \
    --main 120 \
    --cooldown 20 \
    --csv results.csv \
    --report-every 5 \
    --think-ms 50 \
    --tx-timeout 10

# 5. Read-heavy profile
./fb-loadgen \
    --dsn "localhost/3055:e:/Projects_2026/firebirdgotester/EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey \
    --profile read-heavy \
    --conn-init 2 \
    --conn-peak 20 \
    --warmup 30 \
    --main 120 \
    --cooldown 20 \
    --csv results.csv \
    --report-every 5

# 6. Spike profile for stress testing
./fb-loadgen \
    --dsn "localhost/3055:e:/Projects_2026/firebirdgotester/EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey \
    --profile spike \
    --conn-init 5 \
    --conn-peak 40 \
    --warmup 30 \
    --main 120 \
    --cooldown 20 \
    --spike-cycles 3 \
    --spike-hold 15 \
    --think-ms 20 \
    --csv spike_results.csv
```

### Local vs Remote Connections

```bash
# Local Firebird server
./fb-loadgen --profile write-heavy \
    --dsn "localhost/3050:/var/lib/firebird/employee.fdb"

# Remote Firebird server
./fb-loadgen --profile write-heavy \
    --dsn "firebird.example.com/3050:/data/employee.fdb" \
    --user appuser \
    --pass secret123

# Using standard host:port format
./fb-loadgen --profile read-heavy \
    --dsn "192.168.1.100:3055/./EMPLOYEE.FDB" \
    --user SYSDBA \
    --pass masterkey
```

## Command Line Options

### Connection
| Flag | Description | Default |
|------|-------------|---------|
| `--dsn` | Firebird DSN (supports formats: `host/port:database`, `host:port/database`, `host/database`) | `localhost/3055:./EMPLOYEE.FDB` |
| `--user` | Database user | `SYSDBA` |
| `--pass` | Database password | `masterkey` |

**DSN Format Examples:**
- `localhost/3055:./EMPLOYEE.FDB` - Non-standard format (host/port:database)
- `localhost:3055/./EMPLOYEE.FDB` - Standard format (host:port/database)
- `192.168.1.100/3050/var/firebird/employee.fdb` - Remote server

### Profile
| Flag | Description | Default |
|------|-------------|---------|
| `--profile` | Simulation profile: `write-heavy`, `read-heavy`, or `spike` | (required) |

### Connection Scaling
| Flag | Description | Default |
|------|-------------|---------|
| `--conn-init` | Initial number of connections during warmup | `2` |
| `--conn-peak` | Peak number of connections during main phase | `20` |

### Timing (all in seconds)
| Flag | Description | Default |
|------|-------------|---------|
| `--warmup` | Ramp-up / heat period before main load | `30` |
| `--main` | Main steady-state load period | `120` |
| `--cooldown` | Graceful disconnect / ramp-down period | `20` |

### Spike Profile Extras
| Flag | Description | Default |
|------|-------------|---------|
| `--spike-cycles` | Number of spike cycles during main period | `3` |
| `--spike-hold` | Seconds to sustain peak connections before dropping | `10` |

### Output
| Flag | Description | Default |
|------|-------------|---------|
| `--csv` | Path to CSV output file | `results.csv` |
| `--report-every` | Console report interval in seconds | `5` |

### Misc
| Flag | Description | Default |
|------|-------------|---------|
| `--think-ms` | Worker think time between operations in ms | `50` |
| `--tx-timeout` | Statement timeout in seconds | `10` |
| `--dry-run` | Connect, list what would run, exit without load | `false` |

### Help
| Flag | Description |
|------|-------------|
| `--help`, `-h` | Show help message with all options |

## Workload Profiles

### Write-Heavy
- 70% writes, 30% reads
- Focuses on INSERT, UPDATE, DELETE operations
- Tests transaction processing capabilities

### Read-Heavy
- 30% writes, 70% reads
- Focuses on SELECT operations with various complexity
- Tests query performance and caching

### Spike
- Alternates between high and low connection counts
- Tests system behavior under sudden load changes
- Configurable spike cycles and duration

## Operations

### Read Operations
- Customer queries with filters
- Sales order lookups
- Employee information retrieval
- Department and project queries
- Complex joins and aggregations

### Write Operations
- Customer updates (on_hold flag)
- Sales order status changes
- Employee salary adjustments
- New sales order creation
- Transaction-based updates

## Architecture

The load generator follows a modular architecture:

```
main.go
├── config/     - Configuration management
├── db/         - Database connection handling
├── ops/        - Database operations and caching
├── profile/    - Workload profile definitions
├── worker/     - Worker management and metrics
├── ramp/       - Connection scaling logic
└── metrics/    - Metrics collection and reporting
```

### Key Components

- **ConnectionFactory**: Manages database connections with proper limits
- **Cache**: Preloads reference data to avoid contention
- **Profile**: Defines operation mix and timing
- **Scheduler**: Manages worker lifecycle and connection scaling
- **MetricsCollector**: Aggregates performance data
- **Reporter**: Generates reports in multiple formats

## Metrics

The tool collects comprehensive metrics including:

- **Latency**: P95, P99, average response times
- **Throughput**: Operations per second
- **Error Rates**: Failed operations by type
- **Connection Stats**: Pool utilization and wait times
- **Resource Usage**: Memory and CPU metrics

## Output Formats

### Console Output
Real-time progress and summary statistics during execution.

### CSV Output
Detailed metrics suitable for analysis and charting:
```
timestamp,operation,success_count,error_count,avg_latency,p95_latency,p99_latency
2024-01-01T10:00:00Z,SELECT_CUSTOMER,1000,5,15.2,45.1,120.3
```

### JSON Output
Structured data for programmatic processing:
```json
{
  "timestamp": "2024-01-01T10:00:00Z",
  "metrics": {
    "SELECT_CUSTOMER": {
      "success": 1000,
      "errors": 5,
      "latency": {"avg": 15.2, "p95": 45.1, "p99": 120.3}
    }
  }
}
```

## Development

### Adding New Operations

1. Add SQL queries to the appropriate operation file in `ops/`
2. Implement the operation function
3. Register it in the operation factory
4. Update the profile to include the new operation

### Adding New Profiles

1. Create a new profile file in `profile/`
2. Implement the `Profile` interface
3. Register the profile in the profile factory
4. Update documentation

### Testing

```bash
# Run unit tests
go test ./...

# Run with race detection
go test -race ./...

# Run benchmarks
go test -bench=. ./...
```

## Troubleshooting

### Common Issues

1. **Connection Failures**: Verify database is running and accessible
2. **Permission Errors**: Check user credentials and database permissions
3. **Schema Errors**: Ensure EMPLOYEE database is properly set up
4. **Performance Issues**: Adjust connection counts and think times

### Debug Mode

Use `--think-ms 0` and `--tx-timeout 30` for faster debugging cycles.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:
- Create a GitHub issue
- Check the documentation
- Review the example configurations