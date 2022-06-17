# Changelog

## v0.2.1-alpha

* Added `CancelTransaction` feature.

## v0.2.0-alpha

* Added v2 predefined results formats:
  - `GetTransactions` returns `TransactionsAsyncMultipleResponses`.
  - `GetTransaction` returns `TransactionAsyncSingleResponse`.
  - `GetTransactionResults` returns `[]ArrowRelation`.
  - `GetTransactionMetadata` returns `[]TransactionAsyncMetadataResponse`.
  - `GetTransactionProblems` returns `[](ClientProblem|IntegrityConstraintViolation)`.
  - `ExecuteAsync` returns `TransactionAsyncResult`.

* `Problem` model is changed to `ClientProblem` and `IntegrityConstraintViolation`
problem type is introduced.

## v0.1.1-alpha

* Update CreateDatabase and CloneDatabase to use engine-less creation, which
  is a simplification and *breaking change* to the signatures of both functions.

## v0.1.0-alpha
* Initial release
