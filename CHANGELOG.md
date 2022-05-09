# Changelog

## latest

* Added support to the asynchronous protocol including:
    - `ExecuteAsync`: runs an asynchronous transaction.
    - `ExecuteAsyncWait`: runs an asynchronous transaction and wait of its completion.
    - `GetTransaction`: gets information about transaction.
    - `GetTransactions`: gets the list of transactions.
    - `GetTransactionResults`: gets transaction execution results.
    - `GetTransactionMetadata`: gets transaction metadata.
    - `GetTransactionProblems`: gets transaction execution problems.

## v0.1.1-alpha

* Update CreateDatabase and CloneDatabase to use engine-less creation, which
  is a simplification and *breaking change* to the signatures of both functions.

## v0.1.0-alpha
* Initial release
