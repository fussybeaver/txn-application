use async_stream::try_stream;
use csv_async::{AsyncReaderBuilder, ByteRecord};
use futures_util::{Stream, StreamExt};
use tokio::io::AsyncRead;

use crate::{error::ParsingError, model::Transaction};

/// Parse and deserialize a CSV. Errors will occur if the CSV is empty, I/O errors or on faulty
/// deserialization. Receives an `AsyncRead`, so can be swapped into a async TCP server receiving
/// TCP packets, returns a stream of deserialized Transactions.
pub(crate) async fn parse_csv(
    read: impl AsyncRead + Unpin + Send,
) -> impl Stream<Item = Result<Transaction, ParsingError>> {
    let mut rdr = AsyncReaderBuilder::new()
        .trim(csv_async::Trim::All)
        // This parameter seems to be a bug in the csv_async implementation
        .has_headers(false)
        .end_on_io_error(true)
        .create_deserializer(read);

    let mut record = ByteRecord::new();
    try_stream! {
      if rdr.read_byte_record(&mut record).await.map_err(|e| ParsingError::ReadRecord{ record: ByteRecord::clone(&record), source: e })? {
          let mut row = rdr.deserialize();
          while let Some(col) = row.next().await {
            yield col.map_err(|e| ParsingError::Deserialize{ record: ByteRecord::clone(&record), source: e })?;
          }
        } else {
            Err(ParsingError::NoRecords{ record })?
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use rstest::rstest;

    use super::parse_csv;
    use crate::{
        error::ParsingError,
        model::{Transaction, TxType},
    };
    use tokio::io::BufReader;

    #[rstest]
    #[tokio::test]
    #[case::happy(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,1,1,100.0
        withdrawal,1,2,250.0
        "
    }.as_slice(),
        vec![
            Transaction{tx_type: TxType::Deposit, client_id: 1, tx_id: 1, amount: Some(100.)},
            Transaction{tx_type: TxType::Withdrawal, client_id: 1, tx_id: 2, amount: Some(250.)}
        ]
    )]
    #[case::empty(indoc::indoc!{
        b"\
        type,client,tx,amount"
    }.as_slice(), vec![])]
    async fn test_parse_csv(#[case] input: &[u8], #[case] expected: Vec<Transaction>) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to parse");
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[tokio::test]
    #[case::additional_whitespace(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,1,1,100.0
          deposit,2,2 ,200.0
        deposit,1,3,  200.2344666
        withdrawal,1,4,150.0
        "
    }.as_slice(), vec![
            Transaction{tx_type: TxType::Deposit, client_id: 1, tx_id: 1, amount: Some(100.)},
            Transaction{tx_type: TxType::Deposit, client_id: 2, tx_id: 2, amount: Some(200.)},
            Transaction{tx_type: TxType::Deposit, client_id: 1, tx_id: 3, amount: Some(200.23447)},
            Transaction{tx_type: TxType::Withdrawal, client_id: 1, tx_id: 4, amount: Some(150.)}
            
        ])]
    async fn test_parse_csv_whitespace(#[case] input: &[u8], #[case] expected: Vec<Transaction>) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to parse");
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[tokio::test]
    #[case::no_records(indoc::indoc!{
        b""
    }.as_slice())]
    async fn test_parse_csv_no_records(#[case] input: &[u8]) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result.try_collect::<Vec<_>>().await;
        assert!(matches!(actual, Err(ParsingError::NoRecords { .. })));
    }

    #[rstest]
    #[tokio::test]
    #[case::deserialize_missing(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,1
        "
    }.as_slice())]
    async fn test_parse_csv_deserialize(#[case] input: &[u8]) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result.try_collect::<Vec<_>>().await;
        assert!(matches!(actual, Err(ParsingError::Deserialize { .. })));
    }

    #[rstest]
    #[tokio::test]
    #[case::invalid_transaction_type(indoc::indoc!{
        b"\
        type,client,tx,amount
        invalid_type,1,1,100.0
        "
    }.as_slice())]
    async fn test_parse_csv_invalid_transaction_type(#[case] input: &[u8]) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result.try_collect::<Vec<_>>().await;
        assert!(matches!(actual, Err(ParsingError::Deserialize { .. })));
    }

    #[rstest]
    #[tokio::test]
    #[case::invalid_client_id(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,not_a_number,1,100.0
        "
    }.as_slice())]
    async fn test_parse_csv_invalid_client_id(#[case] input: &[u8]) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result.try_collect::<Vec<_>>().await;
        assert!(matches!(actual, Err(ParsingError::Deserialize { .. })));
    }

    #[rstest]
    #[tokio::test]
    #[case::invalid_tx_id(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,1,not_a_number,100.0
        "
    }.as_slice())]
    async fn test_parse_csv_invalid_tx_id(#[case] input: &[u8]) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result.try_collect::<Vec<_>>().await;
        assert!(matches!(actual, Err(ParsingError::Deserialize { .. })));
    }

    #[rstest]
    #[tokio::test]
    #[case::invalid_amount(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,1,1,not_a_number
        "
    }.as_slice())]
    async fn test_parse_csv_invalid_amount(#[case] input: &[u8]) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result.try_collect::<Vec<_>>().await;
        assert!(matches!(actual, Err(ParsingError::Deserialize { .. })));
    }

    #[rstest]
    #[tokio::test]
    #[case::dispute_resolve_chargeback_transactions(indoc::indoc!{
        b"\
        type,client,tx,amount
        dispute,1,1,
        resolve,1,2,
        chargeback,1,3,
        "
    }.as_slice(), vec![
            Transaction{tx_type: TxType::Dispute, client_id: 1, tx_id: 1, amount: None},
            Transaction{tx_type: TxType::Resolve, client_id: 1, tx_id: 2, amount: None},
            Transaction{tx_type: TxType::Chargeback, client_id: 1, tx_id: 3, amount: None}
        ])]
    async fn test_parse_csv_dispute_resolve_chargeback(#[case] input: &[u8], #[case] expected: Vec<Transaction>) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to parse");
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[tokio::test]
    #[case::very_precise_amounts(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,1,1,123.4567
        deposit,2,2,0.0001
        withdrawal,1,3,999999.9999
        "
    }.as_slice(), vec![
            Transaction{tx_type: TxType::Deposit, client_id: 1, tx_id: 1, amount: Some(123.4567)},
            Transaction{tx_type: TxType::Deposit, client_id: 2, tx_id: 2, amount: Some(0.0001)},
            Transaction{tx_type: TxType::Withdrawal, client_id: 1, tx_id: 3, amount: Some(999999.9999)}
        ])]
    async fn test_parse_csv_precise_amounts(#[case] input: &[u8], #[case] expected: Vec<Transaction>) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to parse");
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[tokio::test]
    #[case::max_client_and_tx_ids(indoc::indoc!{
        b"\
        type,client,tx,amount
        deposit,65535,4294967295,100.0
        "
    }.as_slice(), vec![
            Transaction{tx_type: TxType::Deposit, client_id: 65535, tx_id: 4294967295, amount: Some(100.0)}
        ])]
    async fn test_parse_csv_max_ids(#[case] input: &[u8], #[case] expected: Vec<Transaction>) {
        BufReader::new(input);

        let result = parse_csv(input).await;

        let actual = result
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to parse");
        assert_eq!(actual, expected);
    }
}
