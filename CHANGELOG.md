# Changelog

## v0.4.3-alpha

* Add support for Snowflake integrations

## v0.4.2-alpha

## v0.4.1-alpha

* add support for transaction tagging

## v0.4.0-alpha

* Complete overhaul of transaction response handling and results access API.

## v0.3.1-alpha

* Fix v2 show result if empty result.

## v0.3.0-alpha

* Deprecated metadata json format.
* Removed `TransactionAsyncMetadataResponse` model.
* Added support to metadata protobuf format.
* `GetTransactionMetadata` returns protobuf metadata.
* Added `v2` show result functionality.

## v0.2.1

* Renamed:
  * `Execute` to `ExecuteV1`.
  * `ExecuteAsyncWait` to `Execute`.

## v0.2.0

* Added `CancelTransaction` feature.

## v0.1.1

* Added v2 predefined results formats:
  * `GetTransactions` returns `TransactionsAsyncMultipleResponses`.
  * `GetTransaction` returns `TransactionAsyncSingleResponse`.
  * `GetTransactionResults` returns `[]ArrowRelation`.
  * `GetTransactionMetadata` returns `[]TransactionAsyncMetadataResponse`.
  * `GetTransactionProblems` returns `[](ClientProblem|IntegrityConstraintViolation)`.
  * `ExecuteAsync` returns `TransactionAsyncResult`.

* `Problem` model is changed to `ClientProblem` and `IntegrityConstraintViolation`
problem type is introduced.

## v0.1.0

* Update CreateDatabase and CloneDatabase to use engine-less creation, which
  is a simplification and *breaking change* to the signatures of both functions.

## v0.1.0-alpha

* Initial release
