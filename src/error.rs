use std::path::PathBuf;

use csv_async::ByteRecord;

use crate::model::{Amount, ClientId, TxId, TxStatus, TxType};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Couldn't open file for reading: {filename:?}")]
    IOError {
        filename: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ParsingError {
    #[error("Couldn't read record from CSV: {record:?}")]
    ReadRecord {
        record: ByteRecord,
        #[source]
        source: csv_async::Error,
    },
    #[error("Couldn't find data in the CSV: {record:?}")]
    NoRecords { record: ByteRecord },
    #[error("Couldn't deserialize row in CSV: {record:?}")]
    Deserialize {
        record: ByteRecord,
        #[source]
        source: csv_async::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("Transaction must be positive:  Transaction Id '{id}', {tx_type:?} amount {amount}")]
    MustBePositive {
        tx_type: TxType,
        id: TxId,
        amount: Amount,
    },
    #[error(
        "Balance insufficient: available '{available}', {tx_type:?} amount '{amount}', Transaction Id '{id}'"
    )]
    BalanceInsufficient {
        available: Amount,
        tx_type: TxType,
        id: TxId,
        amount: Amount,
    },
    #[error("Locked Account: Client Id '{id}")]
    AccountLocked { id: ClientId },
    #[error("Transaction not found or is invalid for type {tx_type:?}: Transaction Id '{id}'")]
    NotFound { tx_type: TxType, id: TxId },
    #[error("Account not found processing transaction: Client Id '{id}'")]
    AccountNotFound { id: ClientId },
    #[error("Client ID mismatch processing transaction: expected '{expected}', got '{actual}'")]
    ClientIdMismatch {
        expected: ClientId,
        actual: ClientId,
    },
    #[error("Duplicate transaction: Transaction id '{id}'")]
    DuplicateTransaction { id: TxId },
    #[error("Missing amount for transaction type {tx_type:?}: Transaction id '{id}'")]
    MissingAmount { tx_type: TxType, id: TxId },
    #[error(
        "Transaction on incorrect state '{state:?}' for transaction type {tx_type:?}: Transaction id '{id}'"
    )]
    IncorrectState {
        tx_type: TxType,
        state: TxStatus,
        id: TxId,
    },
}
