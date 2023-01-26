// Copyright 2022 RelationalAI, Inc.

package rai

// Global unit tests setup & teardown.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var test struct {
	client       *Client
	databaseName string
	engineName   string
	engineSize   string
	oauthClient  string
	userEmail    string
	noTeardown   bool
	showQuery    bool
}

func fatal(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	os.Stderr.WriteString(msg)
	os.Stderr.WriteString("\n")
	os.Exit(1)
}

func fatalError(err error) {
	fatal(err.Error())
}

func isErrNotFound(err error) bool {
	e, ok := err.(HTTPError)
	if !ok {
		return false
	}
	return e.StatusCode == http.StatusNotFound
}

// Ensure that the test engine exists.
func ensureEngine(client *Client, engine, size string) error {
	fmt.Printf("using engine: %s\n", engine)
	if _, err := client.GetEngine(engine); err != nil {
		if !isErrNotFound(err) {
			return err
		}
		_, err = client.CreateEngine(engine, size)
		if err != nil {
			return err
		}
	}
	return nil
}

// Ensure the test database exists.
func ensureDatabase(client *Client, database string) error {
	fmt.Printf("using database: %s\n", database)
	if _, err := client.GetDatabase(database); err != nil {
		if !isErrNotFound(err) {
			return err
		}
		_, err := client.CreateDatabase(database)
		if err != nil {
			return err
		}
	}
	return nil
}

// http client headers roundTrip
// override the http client default roundTrip
type headerRoundTrip struct {
	defaultRoundTrip http.RoundTripper
	extraHeaders     map[string]string
}

func (h headerRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) {
	for k, v := range h.extraHeaders {
		r.Header.Add(k, v)
	}
	return h.defaultRoundTrip.RoundTrip(r)
}

// todo: fix client init logic, load from config only if env vars are not
// available.
func newTestClient() (*Client, error) {
	configPath, _ := expandUser(DefaultConfigFile)
	var testClient *Client
	if _, err := os.Stat(configPath); err == nil {
		testClient, err = NewDefaultClient()
		if err != nil {
			panic(err)
		}

	} else {
		var cfg Config

		clientId := os.Getenv("CLIENT_ID")
		clientSecret := os.Getenv("CLIENT_SECRET")
		clientCredentialsUrl := os.Getenv("CLIENT_CREDENTIALS_URL")
		raiHost := os.Getenv("HOST")
		if raiHost == "" {
			raiHost = "azure.relationalai.com"
		}

		configFormat := `
		[default]
		host=%s
		region=us-east
		port=443
		scheme=https
		client_id=%s
		client_secret=%s
		client_credentials_url=%s
		`
		configSrc := fmt.Sprintf(configFormat, raiHost, clientId, clientSecret, clientCredentialsUrl)
		if err := LoadConfigString(configSrc, "default", &cfg); err != nil {
			return nil, err
		}
		opts := ClientOptions{Config: cfg}
		testClient = NewClient(context.Background(), &opts)
	}

	// get custom headers
	var customHeaders map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("CUSTOM_HEADERS")), &customHeaders); err == nil {
		fmt.Printf("using custom headers: %s\n", customHeaders)

		// override default http client roundTrip
		var defaultTransport http.RoundTripper
		if testClient.HttpClient.Transport == nil {
			defaultTransport = http.DefaultTransport
		} else {
			defaultTransport = testClient.HttpClient.Transport
		}

		testClient.HttpClient.Transport = headerRoundTrip{
			defaultTransport,
			customHeaders,
		}
	}

	return testClient, nil
}

func tearDown(client *Client) {
	err := client.DeleteDatabase(test.databaseName)
	if err != nil {
		fmt.Println(errors.Wrapf(err, "error deleting database: %s", test.databaseName))
	}
	err = client.DeleteEngine(test.engineName)
	if err != nil {
		fmt.Println(errors.Wrapf(err, "error deleting engine: %s", test.engineName))
	}

	user, _ := client.FindUser(test.userEmail)
	if user != nil {
		_, err := client.DeleteUser(user.ID)
		if err != nil {
			fmt.Println(errors.Wrapf(err, "error deleting user: %s", test.userEmail))
		}
	}

	c, _ := client.FindOAuthClient(test.oauthClient)
	if c != nil {
		_, err := client.DeleteOAuthClient(c.ID)
		if err != nil {
			fmt.Println(errors.Wrapf(err, "error deleting oauth client: %s", test.oauthClient))
		}
	}
}

// Global setup & teardown for golang SDK tests.
func TestMain(m *testing.M) {
	var err error

	// Generating a random email address.
	// Using a common email address can create user creation or deletion issues in edge cases -
	// when tests run in parallel on multiple machines, for example, the CI/CD workflows.
	// Context: https://relationalai.atlassian.net/browse/RAI-9265
	userEmail := fmt.Sprintf("%s@relational.ai", uuid.New().String())
	flag.StringVar(&test.databaseName, "d", "rai-sdk-go", "test database name")
	flag.StringVar(&test.engineName, "e", "rai-sdk-go", "test engine name")
	flag.StringVar(&test.engineSize, "s", "S", "test engine size")
	flag.StringVar(&test.oauthClient, "c", "rai-sdk-go", "test OAuth client name")
	flag.StringVar(&test.userEmail, "u", userEmail, "test user name")
	flag.BoolVar(&test.noTeardown, "no-teardown", false, "don't teardown test resources")
	flag.BoolVar(&test.showQuery, "show-query", false, "display query string")
	flag.Parse()

	test.client, err = newTestClient()
	if err != nil {
		fatalError(err)
	}
	err = ensureEngine(test.client, test.engineName, test.engineSize)
	if err != nil {
		fatalError(err)
	}
	err = ensureDatabase(test.client, test.databaseName)
	if err != nil {
		fatalError(err)
	}
	code := m.Run()
	if !test.noTeardown {
		fmt.Println("Tearing down resources ....")
		tearDown(test.client)
	}
	os.Exit(code)
}
