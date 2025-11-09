use crate::{
    error::TransactionError,
    model::{
        Amount, ClientId, State, Transaction, TransactionExt, TransactionHandler, TxId, TxStatus,
        TxType,
    },
};

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
