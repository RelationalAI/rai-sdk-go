// Copyright 2022 RelationalAI, Inc.

package rai

// Global unit tests setup & teardown.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
)

var testClient *Client

const (
	testDatabaseName    = "rai-sdk-go"
	testEngineName      = "rai-sdk-go"
	testEngineSize      = "S"
	testOAuthClientName = "rai-sdk-go"
	testUserEmail       = "rai-sdk-go@example.com"
)

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
func ensureEngine(client *Client, engine string) error {
	if _, err := client.GetEngine(engine); err != nil {
		if !isErrNotFound(err) {
			return err
		}
		_, err = client.CreateEngine(engine, "S")
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
	client.DeleteDatabase(testDatabaseName)
	client.DeleteEngine(testEngineName)

	user, _ := client.FindUser(testUserEmail)
	if user != nil {
		client.DeleteUser(user.ID)
	}

	c, _ := client.FindOAuthClient(testOAuthClientName)
	if c != nil {
		client.DeleteOAuthClient(c.ID)
	}
}

// Global setup & teardown for golang SDK tests.
func TestMain(m *testing.M) {
	var err error
	testClient, err = newTestClient()
	if err != nil {
		fatalError(err)
	}
	err = ensureEngine(testClient, testEngineName)
	if err != nil {
		fatalError(err)
	}
	err = ensureDatabase(testClient, testDatabaseName)
	if err != nil {
		fatalError(err)
	}
	code := m.Run()
	// tearDown()
	os.Exit(code)
}
