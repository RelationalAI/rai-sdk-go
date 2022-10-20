// Copyright 2022 RelationalAI, Inc.

package rai

// Global unit tests setup & teardown.

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"
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
	var msg string
	msg = fmt.Sprintf(format, args...)
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

// todo: fix client init logic, load from config only if env vars are not
// available.
func newTestClient() (*Client, error) {
	configPath, _ := expandUser(DefaultConfigFile)
	if _, err := os.Stat(configPath); err == nil {
		return NewDefaultClient()
	}

	var cfg Config

	clientId := os.Getenv("CLIENT_ID")
	clientSecret := os.Getenv("CLIENT_SECRET")
	clientCredentialsUrl := os.Getenv("CLIENT_CREDENTIALS_URL")

	placeHolderConfig := `
		[default]
		host=azure.relationalai.com
		region=us-east
		port=443
		scheme=https
		client_id=%s
		client_secret=%s
		client_credentials_url=%s
	`
	configSrc := fmt.Sprintf(placeHolderConfig, clientId, clientSecret, clientCredentialsUrl)
	LoadConfigString(configSrc, "default", &cfg)
	opts := ClientOptions{Config: cfg}
	return NewClient(context.Background(), &opts), nil
}

func tearDown(client *Client) {
	client.DeleteDatabase(test.databaseName)
	client.DeleteEngine(test.engineName)

	user, _ := client.FindUser(test.userEmail)
	if user != nil {
		client.DeleteUser(user.ID)
	}

	c, _ := client.FindOAuthClient(test.oauthClient)
	if c != nil {
		client.DeleteOAuthClient(c.ID)
	}
}

// Global setup & teardown for golang SDK tests.
func TestMain(m *testing.M) {
	var err error

	flag.StringVar(&test.databaseName, "d", "rai-sdk-go", "test database name")
	flag.StringVar(&test.engineName, "e", "rai-sdk-go", "test engine name")
	flag.StringVar(&test.engineSize, "s", "S", "test engine size")
	flag.StringVar(&test.oauthClient, "c", "rai-sdk-go", "test OAuth client name")
	flag.StringVar(&test.userEmail, "u", "rai-sdk-go@relational.ai", "test user name")
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
		tearDown(test.client)
	}
	os.Exit(code)
}
