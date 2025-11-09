use std::collections::HashMap;

use serde::Deserialize;
use strum::{AsRefStr, EnumString};

use crate::error::TransactionError;

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

#[derive(Debug, PartialEq)]
pub struct Deposit {
    inner: Transaction,
    status: TxStatus,
}

impl Deposit {
    pub fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            status: TxStatus::default(),
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct Withdrawal {
    inner: Transaction,
    status: TxStatus,
}

impl Withdrawal {
    pub fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            status: TxStatus::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Dispute {
    inner: Transaction,
    status: TxStatus,
}

impl Dispute {
    pub fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            status: TxStatus::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Resolve {
    inner: Transaction,
    status: TxStatus,
}

impl Resolve {
    pub fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            status: TxStatus::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Chargeback {
    inner: Transaction,
    status: TxStatus,
}

impl Chargeback {
    pub fn new(tx: Transaction) -> Self {
        Self {
            inner: tx,
            status: TxStatus::default(),
        }
    }
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

impl TransactionHandler for Deposit {
    #[inline]
    fn client_id(&self) -> ClientId {
        self.inner.client_id
    }
    #[inline]
    fn tx_id(&self) -> TxId {
        self.inner.tx_id
    }
    #[inline]
    fn tx_type(&self) -> TxType {
        self.inner.tx_type
    }
    #[inline]
    fn amount(&self) -> Option<Amount> {
        self.inner.amount
    }
    #[inline]
    fn status(&self) -> TxStatus {
        self.status
    }
    #[inline]
    fn set_status(&mut self, state: TxStatus) {
        self.status = state;
    }
    fn handle(mut self, state: &mut State) -> Result<(), TransactionError> {
        self.check_duplicate(&state.transactions)?;

        let amount = self.amount().ok_or(TransactionError::MissingAmount {
            tx_type: self.tx_type(),
            id: self.tx_id(),
        })?;

        self.check_positive(amount)?;

        let account = state
            .accounts
            .entry(self.client_id())
            .or_insert_with(|| ClientAccount {
                client_id: self.client_id(),
                ..Default::default()
            });

        self.check_locked(account)?;

        account.available += amount;
        account.total += amount;

        self.status = TxStatus::Valid;

        state.transactions.insert(self.tx_id(), Box::new(self));

        Ok(())
    }
}

impl TransactionHandler for Withdrawal {
    #[inline]
    fn client_id(&self) -> ClientId {
        self.inner.client_id
    }
    #[inline]
    fn tx_id(&self) -> TxId {
        self.inner.tx_id
    }
    #[inline]
    fn tx_type(&self) -> TxType {
        self.inner.tx_type
    }
    #[inline]
    fn amount(&self) -> Option<Amount> {
        self.inner.amount
    }
    #[inline]
    fn status(&self) -> TxStatus {
        self.status
    }
    #[inline]
    fn set_status(&mut self, state: TxStatus) {
        self.status = state;
    }
    fn handle(mut self, state: &mut State) -> Result<(), TransactionError> {
        self.check_duplicate(&state.transactions)?;

        let amount = self.amount().ok_or(TransactionError::MissingAmount {
            tx_type: self.tx_type(),
            id: self.tx_id(),
        })?;

        self.check_positive(amount)?;

        let account = state.accounts.get_mut(&self.client_id()).ok_or_else(|| {
            TransactionError::AccountNotFound {
                id: self.client_id(),
            }
        })?;

        self.check_locked(account)?;

        self.check_sufficient_balance(account.available, amount)?;

        account.available -= amount;
        account.total -= amount;

        self.status = TxStatus::Valid;

        state.transactions.insert(self.tx_id(), Box::new(self));

        Ok(())
    }
}

impl TransactionHandler for Dispute {
    #[inline]
    fn client_id(&self) -> ClientId {
        self.inner.client_id
    }
    #[inline]
    fn tx_id(&self) -> TxId {
        self.inner.tx_id
    }
    #[inline]
    fn tx_type(&self) -> TxType {
        self.inner.tx_type
    }
    #[inline]
    fn amount(&self) -> Option<Amount> {
        self.inner.amount
    }
    #[inline]
    fn status(&self) -> TxStatus {
        self.status
    }
    #[inline]
    fn set_status(&mut self, state: TxStatus) {
        self.status = state;
    }
    fn handle(self, state: &mut State) -> Result<(), TransactionError> {
        let tx = state
            .transactions
            .get_mut(&self.tx_id())
            .filter(|tx| tx.tx_type() == TxType::Deposit)
            .ok_or_else(|| TransactionError::NotFound {
                tx_type: self.tx_type(),
                id: self.tx_id(),
            })
            .and_then(|tx| {
                if TxStatus::Valid != tx.status() {
                    Err(TransactionError::IncorrectState {
                        id: tx.tx_id(),
                        state: tx.status(),
                        tx_type: self.tx_type(),
                    })
                } else {
                    Ok(tx)
                }
            })?;

        self.check_client_id_mismatch(tx.client_id())?;

        let account = state
            .accounts
            .get_mut(&tx.client_id())
            .ok_or_else(|| TransactionError::AccountNotFound { id: tx.client_id() })?;

        self.check_locked(account)?;

        let amount = tx.amount().ok_or_else(|| TransactionError::MissingAmount {
            tx_type: self.tx_type(),
            id: self.tx_id(),
        })?;

        tx.set_status(TxStatus::Disputed);

        // Could result in a negative amount of available funds,
        // we check if we're able to release those funds on the Chargeback transaction

        account.available -= amount;
        account.held += amount;

        Ok(())
    }
}

impl TransactionHandler for Resolve {
    #[inline]
    fn client_id(&self) -> ClientId {
        self.inner.client_id
    }
    #[inline]
    fn tx_id(&self) -> TxId {
        self.inner.tx_id
    }
    #[inline]
    fn tx_type(&self) -> TxType {
        self.inner.tx_type
    }
    #[inline]
    fn amount(&self) -> Option<Amount> {
        self.inner.amount
    }
    #[inline]
    fn status(&self) -> TxStatus {
        self.status
    }
    #[inline]
    fn set_status(&mut self, state: TxStatus) {
        self.status = state;
    }
    fn handle(self, state: &mut State) -> Result<(), TransactionError> {
        let tx = state
            .transactions
            .get_mut(&self.tx_id())
            .filter(|tx| tx.tx_type() == TxType::Deposit)
            .ok_or_else(|| TransactionError::NotFound {
                tx_type: self.tx_type(),
                id: self.tx_id(),
            })
            .and_then(|tx| {
                if TxStatus::Disputed != tx.status() {
                    Err(TransactionError::IncorrectState {
                        id: tx.tx_id(),
                        state: tx.status(),
                        tx_type: self.tx_type(),
                    })
                } else {
                    Ok(tx)
                }
            })?;

        self.check_client_id_mismatch(tx.client_id())?;

        let account = state
            .accounts
            .get_mut(&tx.client_id())
            .ok_or_else(|| TransactionError::AccountNotFound { id: tx.client_id() })?;

        self.check_locked(account)?;

        let amount = tx.amount().ok_or_else(|| TransactionError::MissingAmount {
            tx_type: self.tx_type(),
            id: self.tx_id(),
        })?;

        tx.set_status(TxStatus::Valid);

        self.check_sufficient_balance(account.held, amount)?;

        account.held -= amount;
        account.available += amount;

        Ok(())
    }
}

impl TransactionHandler for Chargeback {
    #[inline]
    fn client_id(&self) -> ClientId {
        self.inner.client_id
    }
    #[inline]
    fn tx_id(&self) -> TxId {
        self.inner.tx_id
    }
    #[inline]
    fn tx_type(&self) -> TxType {
        self.inner.tx_type
    }
    #[inline]
    fn amount(&self) -> Option<Amount> {
        self.inner.amount
    }
    #[inline]
    fn status(&self) -> TxStatus {
        self.status
    }
    #[inline]
    fn set_status(&mut self, state: TxStatus) {
        self.status = state;
    }
    fn handle(self, state: &mut State) -> Result<(), TransactionError> {
        let tx = state
            .transactions
            .get_mut(&self.tx_id())
            .filter(|tx| tx.tx_type() == TxType::Deposit)
            .ok_or_else(|| TransactionError::NotFound {
                tx_type: self.tx_type(),
                id: self.tx_id(),
            })
            .and_then(|tx| {
                if TxStatus::Disputed != tx.status() {
                    Err(TransactionError::IncorrectState {
                        id: tx.tx_id(),
                        state: tx.status(),
                        tx_type: self.tx_type(),
                    })
                } else {
                    Ok(tx)
                }
            })?;

        self.check_client_id_mismatch(tx.client_id())?;

        let account = state
            .accounts
            .get_mut(&tx.client_id())
            .ok_or_else(|| TransactionError::AccountNotFound { id: tx.client_id() })?;

        self.check_locked(account)?;

        let amount = tx.amount().ok_or_else(|| TransactionError::MissingAmount {
            tx_type: self.tx_type(),
            id: self.tx_id(),
        })?;

        tx.set_status(TxStatus::Chargeback);

        // Check if a previous dispute left the account in arrears
        // and should fail the chargeback due to a negative balance
        if account.available < 0. {
            return Err(TransactionError::BalanceInsufficient {
                available: account.available + amount,
                tx_type: self.tx_type(),
                id: self.tx_id(),
                amount,
            });
        }

        self.check_sufficient_balance(account.held, amount)?;

        account.held -= amount;
        account.total -= amount;
        account.locked = true;

        Ok(())
    }
}
