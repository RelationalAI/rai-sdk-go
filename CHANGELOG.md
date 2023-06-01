# Changelog
## v0.5.4-alpha
* Add Snowflake role in database links

## v0.5.3-alpha
* Make passing SF creds optional for creating/deleting data streams

## v0.5.2-alpha
* Add support for updating Snowflake database links

## v0.5.1-alpha
* removing engine since it is no longer required by snowflake integrations.

## v0.5.0-alpha
  * remove extraneous integration parameters from the metadata (engine, objecttype)
  * Add support for updating credentials used in Snowflake integrations


## v0.4.7-alpha

* Add support for Snowflake data stream status.

## v0.4.6-alpha

* Add support for Snowflake data stream on views.

## v0.4.5-alpha

* Add support for Snowflake data stream APIs.

## v0.4.4-alpha

* Add support for Snowflake database links.

## v0.4.3-alpha

* Add support for Snowflake integrations.

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
