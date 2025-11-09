# Transaction Processing Engine

A robust payments engine designed to handle data from CSV files, processing different types of transactions for Client balances.

## Run the application

```bash
> cargo run -- <FILENAME>
Usage:

Arguments:
  <FILENAME>  CSV file to parse

Options:
  -v, --verbose
  -h, --help     Print help
  -V, --version  Print version
```

The verbose flag will emit transaction errors that occur during processing.

## Implementation Details

The application utilizes Tokio's asynchronous runtime with streaming CSV
processing to ensure scalable performance.

The decision to use `async_csv` balances the desire to enable CSV parsing
inside the Tokio scheduler, maintainability of the CSV parsing logic and the
risk of using a less mature crate.

Alternatives, such as the more mature `csv` crate requires careful balancing
with the Tokio's external thread scheduling (using `spawn_blocking`) and the
rest of the application, whereas writing a CSV Parser ourselves reduces the
maintainability of this application, should we need to add further fields to
the application. Some performance is lost due to converting to UTF-8 strings
prior to deserialization.

CSV parsing returns a tokio stream, so should run in constant memory and will
not be affected by file size.

Separate handlers for each transaction type allow modularity should another
transaction type be introduced.

### Error Handling

Custom error types allow the application to debug errors effectively, most are
amended with a transaction id. The transaction errors covered are:

- Missing transaction amounts
- Insufficient funds
- Duplicate transactions
- References to non-existent transactions
- Client ID mismatches
- Operations on locked accounts
- Disputes on withdrawal transactions
- Chargebacks/resolves on non-disputed transactions

### Unit Tests

Comprehensive unit coverage is available through:

```bash
cargo test
```

## Error Recovery

Transaction errors are skipped, and optionally logged to stderr (`-v` flag),
ensuring that faulty transactions continue processing the file.

## Performance Considerations

Memory increases with the number of unique clients and transactions, but not
the file size: O(c + t) where c is the number of clients and t is the number of
stored transactions. The CSV parsing logic receives an `AsyncRead` and can be
used in concurrent processing, allowing processing of arbitrarily large files.
