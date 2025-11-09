//! Entrypoint binary to the transaction application.

use std::path::PathBuf;

use clap::{Parser, command};

#[derive(Debug, Parser)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(short, long)]
    verbose: bool,
    /// CSV file to parse
    filename: PathBuf,
}

use std::path::Path;

use futures_util::StreamExt;

use crate::error::Error;
use crate::model::{
    State, TransactionHandler, TxType, chargeback::Chargeback, deposit::Deposit, dispute::Dispute,
    resolve::Resolve, withdrawal::Withdrawal,
};

mod csv;
mod error;
mod model;

/// Runs the application, reading the CSV file and parsing transactions. CSV parsing errors and
/// File I/O errors are bubbled up, whereas Transaction errors are optionally logged and skipped to
/// process the entire file.
pub async fn run(file: impl AsRef<Path>, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    let fp = tokio::fs::File::open(&file)
        .await
        .map_err(|e| Error::IOError {
            filename: file.as_ref().to_path_buf(),
            source: e,
        })?;

    let stream = csv::parse_csv(fp).await;

    let mut state = State::default();
    futures_util::pin_mut!(stream);
    while let Some(transaction) = stream.next().await {
        let tx = transaction?;
        let res = match tx.tx_type {
            TxType::Deposit => Deposit::new(tx).handle(&mut state),
            TxType::Withdrawal => Withdrawal::new(tx).handle(&mut state),
            TxType::Resolve => Resolve::new(tx).handle(&mut state),
            TxType::Chargeback => Chargeback::new(tx).handle(&mut state),
            TxType::Dispute => Dispute::new(tx).handle(&mut state),
        };

        match res {
            Ok(_) => {}
            // We skip transaction errors and continue processing
            Err(e) => {
                if verbose {
                    eprintln!("{e}")
                }
            }
        }
    }

    for balance in state.accounts.into_values() {
        println!(
            "{},{},{},{},{}",
            balance.client_id,
            fmt_decimals(balance.available),
            fmt_decimals(balance.held),
            fmt_decimals(balance.total),
            balance.locked
        );
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(args.filename, args.verbose).await {
        Ok(_) => (),
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1)
        }
    }
}

fn fmt_decimals(value: f32) -> String {
    let formatted = format!("{:.4}", value);

    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

#[cfg(test)]
mod tests {

    use rstest::rstest;

    use crate::{
        error::TransactionError,
        model::{Transaction, TxType},
    };

    use super::*;

    #[rstest]
    #[case::deposit(
        Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.),
        }),
        Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.),
        })
    )]
    fn test_deposit(#[case] deposit1: Deposit, #[case] deposit2: Deposit) {
        let mut state = State::default();

        deposit1.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 100.);
        assert_eq!(state.accounts[&1].held, 0.);
        assert_eq!(state.accounts[&1].total, 100.);
        assert!(!state.accounts[&1].locked);
        assert_eq!(state.transactions[&1].tx_type(), TxType::Deposit);
        assert_eq!(state.transactions[&1].tx_id(), 1);
        assert_eq!(state.transactions[&1].client_id(), 1);
        assert_eq!(state.transactions[&1].amount(), Some(100.));

        deposit2.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 150.);
        assert_eq!(state.accounts[&1].held, 0.);
        assert_eq!(state.accounts[&1].total, 150.);
        assert!(!state.accounts[&1].locked);
    }

    #[test]
    fn test_deposit_multi_user() {
        let mut state = State::default();

        let deposit1 = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        let deposit2 = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 2,
            client_id: 2,
            amount: Some(200.0),
        });

        deposit1.handle(&mut state).unwrap();

        deposit2.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 100.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 100.0);
        assert!(!state.accounts[&1].locked);
        assert_eq!(state.transactions[&1].tx_type(), TxType::Deposit);
        assert_eq!(state.transactions[&1].tx_id(), 1);
        assert_eq!(state.transactions[&1].client_id(), 1);
        assert_eq!(state.transactions[&1].amount(), Some(100.0));

        assert_eq!(state.accounts[&2].available, 200.0);
        assert_eq!(state.accounts[&2].held, 0.0);
        assert_eq!(state.accounts[&2].total, 200.0);
        assert!(!state.accounts[&2].locked);
        assert_eq!(state.transactions[&2].tx_type(), TxType::Deposit);
        assert_eq!(state.transactions[&2].tx_id(), 2);
        assert_eq!(state.transactions[&2].client_id(), 2);
        assert_eq!(state.transactions[&2].amount(), Some(200.0));
    }
    #[test]
    fn test_withdrawal() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        withdrawal.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 50.0);
        assert!(!state.accounts[&1].locked);
        assert_eq!(state.transactions[&2].tx_type(), TxType::Withdrawal);
        assert_eq!(state.transactions[&2].tx_id(), 2);
        assert_eq!(state.transactions[&2].client_id(), 1);
        assert_eq!(state.transactions[&2].amount(), Some(50.0));
    }

    #[test]
    fn test_withdrawal_overdraw() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(101.0),
        });

        let res = withdrawal.handle(&mut state);

        assert!(matches!(
            res,
            Err(TransactionError::BalanceInsufficient { .. })
        ));

        assert_eq!(state.accounts[&1].available, 100.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 100.0);
        assert!(!state.accounts[&1].locked);
    }

    #[test]
    fn test_withdrawal_from_nonexistent_account() {
        let mut state = State::default();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        let res = withdrawal.handle(&mut state);

        assert!(matches!(
            res,
            Err(TransactionError::AccountNotFound { id: 1 })
        ));

        // Ensure no account was created
        assert!(!state.accounts.contains_key(&1));
    }

    #[test]
    fn test_duplicate_transaction() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        // Duplicate - attempt to process same transaction ID again
        let duplicate_deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1, // Same tx_id
            client_id: 1,
            amount: Some(100.0),
        });

        let res = duplicate_deposit.handle(&mut state);

        assert!(matches!(
            res,
            Err(TransactionError::DuplicateTransaction { id: 1 })
        ));

        assert_eq!(state.accounts[&1].available, 100.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 100.0);
        assert!(!state.accounts[&1].locked);
    }

    #[test]
    fn test_negative_amount_deposit() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(-100.0),
        });

        let res = deposit.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::MustBePositive { .. })));

        // Ensure no account was created
        assert!(!state.accounts.contains_key(&1));
    }

    #[test]
    fn test_negative_amount_withdrawal() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });
        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(-50.0),
        });

        let res = withdrawal.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::MustBePositive { .. })));

        // Balance should remain unchanged
        assert_eq!(state.accounts[&1].available, 100.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 100.0);
    }

    #[test]
    fn test_deposit_missing_amount() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        let res = deposit.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::MissingAmount { .. })));

        // Ensure no account was created
        assert!(!state.accounts.contains_key(&1));
    }

    #[test]
    fn test_withdrawal_missing_amount() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });
        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: None,
        });

        let res = withdrawal.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::MissingAmount { .. })));

        // Balance should remain unchanged
        assert_eq!(state.accounts[&1].available, 100.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 100.0);
    }

    #[test]
    fn test_dispute_non_existent_tx() {
        let mut state = State::default();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        let res = dispute.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::NotFound { .. })));
    }

    #[test]
    fn test_dispute_client_mismatch() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            // Client ID does not match.
            client_id: 2,
            amount: None,
        });

        let res = dispute.handle(&mut state);

        assert!(matches!(
            res,
            Err(TransactionError::ClientIdMismatch { .. })
        ));
    }

    #[test]
    fn test_dispute_transaction() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        deposit.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        dispute.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 150.0);
    }

    #[test]
    fn test_dispute_withdrawal() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let deposit = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        deposit.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 2,
            client_id: 1,
            amount: None,
        });

        let res = dispute.handle(&mut state);

        // In our implementation it's not allowed to dispute a Withdrawal
        assert!(
            matches!(res, Err(TransactionError::NotFound { .. })),
            "{:?}",
            res
        );

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 50.0);
    }

    #[test]
    fn test_resolve_non_existent_tx() {
        let mut state = State::default();

        let resolve = Resolve::new(Transaction {
            tx_type: TxType::Resolve,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        let res = resolve.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::NotFound { .. })));
    }

    #[test]
    fn test_resolve_incorrect_state() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let resolve = Resolve::new(Transaction {
            tx_type: TxType::Resolve,
            tx_id: 1,
            // Client ID does not match.
            client_id: 2,
            amount: None,
        });

        let res = resolve.handle(&mut state);

        assert!(
            matches!(res, Err(TransactionError::IncorrectState { .. })),
            "{:?}",
            res
        );
    }

    #[test]
    fn test_chargeback_non_existent_tx() {
        let mut state = State::default();

        let chargeback = Chargeback::new(Transaction {
            tx_type: TxType::Chargeback,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        let res = chargeback.handle(&mut state);

        assert!(matches!(res, Err(TransactionError::NotFound { .. })));
    }

    #[test]
    fn test_chargeback_non_disputed_transaction() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });
        deposit.handle(&mut state).unwrap();

        // Try chargeback without dispute first
        let chargeback = Chargeback::new(Transaction {
            tx_type: TxType::Chargeback,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });
        let res = chargeback.handle(&mut state);

        // Should fail because a Chargeback needs to be disputed first
        assert!(
            matches!(res, Err(TransactionError::IncorrectState { .. })),
            "{:?}",
            res
        );

        assert_eq!(state.accounts[&1].available, 100.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 100.0);
        assert!(!state.accounts[&1].locked);
    }

    #[test]
    fn test_chargeback_transaction() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        deposit.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        dispute.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 150.0);

        let chargeback = Chargeback::new(Transaction {
            tx_type: TxType::Chargeback,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        chargeback.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 50.0);
        assert!(state.accounts[&1].locked);
    }

    #[test]
    fn test_chargeback_transaction_negative_balance_resolve() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        withdrawal.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        dispute.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, -50.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 50.0);

        let resolve = Resolve::new(Transaction {
            tx_type: TxType::Resolve,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        resolve.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 50.0);
    }

    #[test]
    fn test_chargeback_transaction_negative_balance_failed_chargeback() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        withdrawal.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        dispute.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, -50.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 50.0);

        let chargeback = Chargeback::new(Transaction {
            tx_type: TxType::Chargeback,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        let res = chargeback.handle(&mut state);

        // Should fail because there are not enough funds available to chargeback
        assert!(
            matches!(res, Err(TransactionError::BalanceInsufficient { .. })),
            "{:?}",
            res
        );

        assert_eq!(state.accounts[&1].available, -50.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 50.0);
    }

    #[test]
    fn test_chargeback_transaction_negative_balance_chargeback_on_resolved() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });

        deposit.handle(&mut state).unwrap();

        let withdrawal = Withdrawal::new(Transaction {
            tx_type: TxType::Withdrawal,
            tx_id: 2,
            client_id: 1,
            amount: Some(50.0),
        });

        withdrawal.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        dispute.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, -50.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 50.0);

        let resolve = Resolve::new(Transaction {
            tx_type: TxType::Resolve,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        resolve.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 50.0);

        let chargeback = Chargeback::new(Transaction {
            tx_type: TxType::Chargeback,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });

        let res = chargeback.handle(&mut state);

        // Should fail because we can't chargeback a resolved dispute
        assert!(
            matches!(res, Err(TransactionError::IncorrectState { .. })),
            "{:?}",
            res
        );

        assert_eq!(state.accounts[&1].available, 50.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 50.0);
    }

    #[test]
    fn test_account_locked_after_chargeback() {
        let mut state = State::default();

        let deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 1,
            client_id: 1,
            amount: Some(100.0),
        });
        deposit.handle(&mut state).unwrap();

        let dispute = Dispute::new(Transaction {
            tx_type: TxType::Dispute,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });
        dispute.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 0.0);
        assert_eq!(state.accounts[&1].held, 100.0);
        assert_eq!(state.accounts[&1].total, 100.0);
        assert!(!state.accounts[&1].locked);

        let chargeback = Chargeback::new(Transaction {
            tx_type: TxType::Chargeback,
            tx_id: 1,
            client_id: 1,
            amount: None,
        });
        chargeback.handle(&mut state).unwrap();

        assert_eq!(state.accounts[&1].available, 0.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 0.0);
        assert!(state.accounts[&1].locked);

        let new_deposit = Deposit::new(Transaction {
            tx_type: TxType::Deposit,
            tx_id: 3,
            client_id: 1,
            amount: Some(50.0),
        });

        let res = new_deposit.handle(&mut state);
        assert!(matches!(
            res,
            Err(TransactionError::AccountLocked { id: 1 })
        ));

        assert_eq!(state.accounts[&1].available, 0.0);
        assert_eq!(state.accounts[&1].held, 0.0);
        assert_eq!(state.accounts[&1].total, 0.0);
        assert!(state.accounts[&1].locked);
    }
}
