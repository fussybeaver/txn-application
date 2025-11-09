# Transaction Processing Engine

A payments engine designed to handle data from CSV files, processing different
types of transactions, manage Client account balances and dealing with disputes.

## Run the application

The CLI application takes a single file as a path argument and prints the
resulting account states to `stdout`.

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

The application processes the CSV as a stream allowing it to run in constant
memory relative to the CSV file size. Memory is bound by the number of unique
clients and the number of transactions.

The CSV parser is using `AsyncRead` to allow core logic to be adapted for other
concurrent usage scenarios.

### CSV parsing

The decision to use the `async-csv` dependency to handle parsing balances the
desire to enable non-blocking I/O and maintainability of the CSV parsing logic
against the risk of using a less mature crate.

Alternatives considered were: using the more mature `csv` crate, which requires
careful balancing with the Tokio's external thread scheduling due to the
crate's blocking I/O implementation; or hand-writing a CSV Parser (the more
performant option), which reduces the maintainability of this application. 

### Edge cases

Separate handlers for each transaction type allow modularity should another
transaction type be introduced.

The following assumptions were made to ensure fault-free processing:

 - A Dispute transaction can only reference a Deposit transaction.
 - A Dispute transaction that renders the account 'available' counter in
   arrears (through a combination of Deposits and Withdrawals) will not fail,
   but will potentially fail on the subsequent Chargeback.
 - A Resolve transaction can only reference a prior Dispute transaction.
 - A Chargeback transaction can only reference a prior Dispute transaction.
 - No operation unlocks a Client account after being locked from a successful
   Chargeback.

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

