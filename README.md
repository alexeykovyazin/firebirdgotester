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
# Run with default settings
./fb-loadgen --profile write-heavy

# Custom connection string
./fb-loadgen --profile read-heavy --conn "localhost/3055:./EMPLOYEE.FDB" --user SYSDBA --pass masterkey

# Spike testing with custom timing
./fb-loadgen --profile spike --warmup 10 --main 60 --cooldown 10 --spike-cycles 2 --spike-hold 5

# Output to CSV
./fb-loadgen --profile write-heavy --csv results.csv --report-every 10
```

## Command Line Options

### Connection
- `--dsn`: Firebird DSN (default: "localhost/3055:./EMPLOYEE.FDB")
- `--user`: DB user (default: "SYSDBA")
- `--pass`: DB password (default: "masterkey")

### Profile
- `--profile`: Simulation profile (required: write-heavy|read-heavy|spike)

### Connection Scaling
- `--conn-init`: Initial number of connections (default: 2)
- `--conn-peak`: Peak number of connections (default: 20)

### Timing (all in seconds)
- `--warmup`: Ramp-up period (default: 30)
- `--main`: Main steady-state period (default: 120)
- `--cooldown`: Graceful disconnect period (default: 20)

### Spike Profile Extras
- `--spike-cycles`: Number of spike cycles (default: 3)
- `--spike-hold`: Seconds to sustain peak (default: 10)

### Output
- `--csv`: CSV output file (default: "results.csv")
- `--report-every`: Console report interval in seconds (default: 5)

### Misc
- `--think-ms`: Worker think time between ops in ms (default: 50)
- `--tx-timeout`: Statement timeout in seconds (default: 10)
- `--dry-run`: Connect, list what would run, exit (default: false)

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