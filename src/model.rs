use std::collections::HashMap;

use serde::Deserialize;
use strum::{AsRefStr, EnumString};

use crate::error::TransactionError;

pub mod chargeback;
pub mod deposit;
pub mod dispute;
pub mod resolve;
pub mod withdrawal;

pub type ClientId = u16;
pub type TxId = u32;
pub type Amount = f32;

/// Represents the Transaction type.
#[derive(Copy, Clone, Debug, PartialEq, AsRefStr, EnumString, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Holds the mutable world state for the application, including Client accounts and previous
/// transactions.
#[derive(Default)]
pub struct State {
    pub accounts: HashMap<ClientId, ClientAccount>,
    pub transactions: HashMap<TxId, Box<dyn TransactionHandler>>,
}

/// Identifies a Transaction as desieralized from the CSV file.
#[derive(Debug, Deserialize, PartialEq)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub tx_type: TxType,
    pub client_id: u16,
    pub tx_id: u32,
    pub amount: Option<f32>,
}

/// Embodies a Client account with a total balance, funds available to withdraw and funds held
/// against chargebacks. A client account will be locked on a Chargeback transaction, which
/// prevents further operations on that Client. This implementation never unlocks a client.
#[derive(Debug, Default)]
pub struct ClientAccount {
    pub client_id: u16,
    pub available: f32,
    pub held: f32,
    pub total: f32,
    pub locked: bool,
}

/// A deposit transaction can have a status, dispute, resolve and chargeback transactions can only
/// operate on target states:
///
///  - Dispute requires a Valid state, and sets the Deposit transaction into a Disputed state.
///  - Resolve requires a Disputed state, and sets the Deposit transaction back to Valid state.
///  - Chargeback requires a Disputed state, and sets the Deposit transaction to a Chargeback state.
#[derive(Copy, Clone, Debug, Default, PartialEq, AsRefStr, EnumString)]
pub enum TxStatus {
    #[default]
    Valid,
    Disputed,
    Chargeback,
}

pub trait TransactionHandler {
    fn client_id(&self) -> ClientId;
    fn tx_id(&self) -> TxId;
    fn tx_type(&self) -> TxType;
    fn amount(&self) -> Option<Amount>;
    fn status(&self) -> TxStatus;
    fn set_status(&mut self, state: TxStatus);

    /// Processes a transaction and updates the application's State.
    fn handle(self, state: &mut State) -> Result<(), TransactionError>;
}

trait TransactionExt {
    fn check_positive(&self, amount: Amount) -> Result<(), TransactionError>;
    fn check_sufficient_balance(
        &self,
        available: Amount,
        amount: Amount,
    ) -> Result<(), TransactionError>;
    fn check_client_id_mismatch(&self, client_id: ClientId) -> Result<(), TransactionError>;
    fn check_duplicate(
        &self,
        transactions: &HashMap<TxId, Box<dyn TransactionHandler>>,
    ) -> Result<(), TransactionError>;
    fn check_locked(&self, account: &ClientAccount) -> Result<(), TransactionError>;
}

impl<T: TransactionHandler> TransactionExt for T {
    /// Returns a MustBePositive error if the balance is below zero.
    fn check_positive(&self, amount: Amount) -> Result<(), TransactionError> {
        if amount < 0.0 {
            Err(TransactionError::MustBePositive {
                tx_type: self.tx_type(),
                id: self.tx_id(),
                amount,
            })
        } else {
            Ok(())
        }
    }

    /// Returns a BalanceInsufficient error if the balance is below a certain amount.
    fn check_sufficient_balance(
        &self,
        available: Amount,
        amount: Amount,
    ) -> Result<(), TransactionError> {
        if available < amount {
            Err(TransactionError::BalanceInsufficient {
                available,
                tx_type: self.tx_type(),
                id: self.tx_id(),
                amount,
            })
        } else {
            Ok(())
        }
    }

    /// Returns a ClientIdMismatch error if the Client Id doesn't match this transaction's Client
    /// Id.
    fn check_client_id_mismatch(&self, client_id: ClientId) -> Result<(), TransactionError> {
        if client_id != self.client_id() {
            Err(TransactionError::ClientIdMismatch {
                expected: client_id,
                actual: self.client_id(),
            })
        } else {
            Ok(())
        }
    }

    /// Returns a DuplicateTransaction error if the Deposit or Withdrawal has an identical
    /// transaction id in the State.
    fn check_duplicate(
        &self,
        transactions: &HashMap<TxId, Box<dyn TransactionHandler>>,
    ) -> Result<(), TransactionError> {
        if transactions.keys().any(|tx| *tx == self.tx_id()) {
            Err(TransactionError::DuplicateTransaction { id: self.tx_id() })
        } else {
            Ok(())
        }
    }

    /// Returns an AccountLocked error if the Client Account was locked through a successful
    /// Chargeback transaction.
    fn check_locked(&self, account: &ClientAccount) -> Result<(), TransactionError> {
        if account.locked {
            Err(TransactionError::AccountLocked {
                id: self.client_id(),
            })
        } else {
            Ok(())
        }
    }
}
