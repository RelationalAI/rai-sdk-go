# rai-sdk-go

![build](https://github.com/RelationalAI/rai-sdk-go/actions/workflows/go-build.yaml/badge.svg)

The RelationalAI Software Development Kit for Go enables developers to access the RAI REST APIs from Go.

* You can find RelationalAI Go SDK documentation at <https://docs.relational.ai/rkgms/sdk/go-sdk>
* You can find RelationalAI product documentation at <https://docs.relational.ai>
* You can learn more about RelationalAI at <https://relational.ai>

## Getting started

### Requirements

* Go 1.18+

### Compile the SDK

    cd ./rai
    go build

### Run the tests

    ./run-tests

Note, the test are run against the account configured in your SDK config file.

The tests take several optional arguments, which can sometimes make
itereating and debugging easier.

    ./run-tests -args <arguments>

| Argument        | Description |
|-----------------|-------------|
| -d \<database\> | Test database name |
| -e \<engine\>   | Test engine name   |
| -s \<size\>     | Test engine size   |
| -c \<client\>   | Test OAuth client name |
| -u \<user\>     | Test user email address |
| -no-teardown    | Don't teardown test resouces |
| -show-query     | Show query string for all results tests |

The `-no-teardown` option can be helpful when iterating on tests, because
when combined with a given database name, it avoids recreating the engine on
every test run.

The `-show-query` option is useful when narrowing down failures in the results
tests.

And these can also be passed directly to `go test`.

### Create a configuration file

In order to run the examples you will need to create an SDK config file.
The default location for the file is `$HOME/.rai/config` and the file should
include the following:

Sample configuration using OAuth client credentials:

    [default]
    host = azure.relationalai.com
    port = <api-port>      # optional, default: 443
    scheme = <scheme>      # optional, default: https
    client_id = <your client_id>
    client_secret = <your client secret>
    client_credentials_url = <account login URL>  # optional
    # default: https://login.relationalai.com/oauth/token

Client credentials can be created using the RAI console at
<https://console.relationalai.com/login>

You can copy `config.spec` from the root of this repo and modify as needed.

## Generate golang sources from protobuf specification

    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    export PATH=$PATH:$HOME/go/bin
    protoc -I protos --go_out=. --go_opt=Mschema.proto=./rai/pb --go_opt=Mmessage.proto=./rai/pb protos/*.proto

## Examples

The SDK contains examples for every API, and various other SDK features. These
are located in `./examples` folder.

Each example can be run using the `go` command.

    cd ./examples
    go run get_database/main.go -d sdk-test

There is also a bash script in `./examples` that can be used to run
individual examples.

    cd ./examples
    ./run get_database -d sdk-test

## Support

You can reach the RAI developer support team at `support@relational.ai`

## Contributing

We value feedback and contributions from our developer community. Feel free
to submit an issue or a PR here.

## License

The RelationalAI Software Development Kit for Go is licensed under the
Apache License 2.0. See:
<https://github.com/RelationalAI/rai-sdk-go/blob/master/LICENSE>
