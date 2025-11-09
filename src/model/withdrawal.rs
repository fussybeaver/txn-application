use crate::{
    error::TransactionError,
    model::{
        Amount, ClientId, State, Transaction, TransactionExt, TransactionHandler, TxId, TxStatus,
        TxType,
    },
};

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
