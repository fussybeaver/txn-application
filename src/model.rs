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

#[derive(Copy, Clone, Debug, PartialEq, AsRefStr, EnumString, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Default)]
pub struct State {
    pub accounts: HashMap<ClientId, ClientAccount>,
    pub transactions: HashMap<TxId, Box<dyn TransactionHandler>>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub tx_type: TxType,
    pub client_id: u16,
    pub tx_id: u32,
    pub amount: Option<f32>,
}

#[derive(Debug, Default)]
pub struct ClientAccount {
    pub client_id: u16,
    pub available: f32,
    pub held: f32,
    pub total: f32,
    pub locked: bool,
}

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
