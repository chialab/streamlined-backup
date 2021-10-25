package config

import (
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	testAwsProfile         = "streamlined-backup-test"
	testAwsAccessKeyId     = "AKIAIOSFODNN7EXAMPLE"
	testAwsSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testAwsSessionToken    = "session-token"
)

func TestClient(t *testing.T) {
	t.Run("static_credentials", func(t *testing.T) {
		t.Parallel()

		dest := &S3DestinationDefinition{
			Bucket: "example-bucket",
			Region: "eu-south-1",
			Credentials: &S3Credentials{
				AccessKeyId:     testAwsAccessKeyId,
				SecretAccessKey: testAwsSecretAccessKey,
				SessionToken:    testAwsSessionToken,
			},
		}

		client := dest.Client()
		if client == nil {
			t.Fatalf("expected client to be created")
		}
		if *client.Config.Region != dest.Region {
			t.Errorf("expected region %s, got %s", dest.Region, *client.Config.Region)
		}

		credentials, err := client.Config.Credentials.Get()
		if err != nil {
			t.Fatalf("expected credentials to be set, got %s", err)
		}
		if credentials.AccessKeyID != testAwsAccessKeyId {
			t.Errorf("expected access key id %s, got %s", testAwsAccessKeyId, credentials.AccessKeyID)
		}
		if credentials.SecretAccessKey != testAwsSecretAccessKey {
			t.Errorf("expected secret access key %s, got %s", testAwsSecretAccessKey, credentials.SecretAccessKey)
		}
		if credentials.SessionToken != testAwsSessionToken {
			t.Errorf("expected session token %s, got %s", testAwsSessionToken, credentials.SessionToken)
		}
	})

	t.Run("profile", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpFile := tmpDir + "/credentials"
		data := fmt.Sprintf("[%s]\naws_access_key_id = %s\naws_secret_access_key = %s\n", testAwsProfile, testAwsAccessKeyId, testAwsSecretAccessKey)
		if err := os.WriteFile(tmpFile, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", tmpFile)

		dest := &S3DestinationDefinition{
			Bucket:  "example-bucket",
			Region:  "eu-west-1",
			Profile: &testAwsProfile,
		}

		client := dest.Client()
		if client == nil {
			t.Fatalf("expected client to be created")
		}
		if *client.Config.Region != dest.Region {
			t.Errorf("expected region %s, got %s", dest.Region, *client.Config.Region)
		}

		credentials, err := client.Config.Credentials.Get()
		if err != nil {
			t.Fatalf("expected credentials to be set, got %s", err)
		}
		if credentials.AccessKeyID != testAwsAccessKeyId {
			t.Errorf("expected access key id %s, got %s", testAwsAccessKeyId, credentials.AccessKeyID)
		}
		if credentials.SecretAccessKey != testAwsSecretAccessKey {
			t.Errorf("expected secret access key %s, got %s", testAwsSecretAccessKey, credentials.SecretAccessKey)
		}
		if credentials.SessionToken != "" {
			t.Errorf("expected session token %s, got %s", "", credentials.SessionToken)
		}
	})

	t.Run("default_env", func(t *testing.T) {
		t.Setenv("AWS_ACCESS_KEY_ID", testAwsAccessKeyId)
		t.Setenv("AWS_SECRET_ACCESS_KEY", testAwsSecretAccessKey)
		t.Setenv("AWS_SESSION_TOKEN", testAwsSessionToken)

		dest := &S3DestinationDefinition{
			Bucket: "example-bucket",
			Region: "eu-south-1",
		}

		client := dest.Client()
		if client == nil {
			t.Fatalf("expected client to be created")
		}
		if *client.Config.Region != dest.Region {
			t.Errorf("expected region %s, got %s", dest.Region, *client.Config.Region)
		}

		credentials, err := client.Config.Credentials.Get()
		if err != nil {
			t.Fatalf("expected credentials to be set, got %s", err)
		}
		if credentials.AccessKeyID != testAwsAccessKeyId {
			t.Errorf("expected access key id %s, got %s", testAwsAccessKeyId, credentials.AccessKeyID)
		}
		if credentials.SecretAccessKey != testAwsSecretAccessKey {
			t.Errorf("expected secret access key %s, got %s", testAwsSecretAccessKey, credentials.SecretAccessKey)
		}
		if credentials.SessionToken != testAwsSessionToken {
			t.Errorf("expected session token %s, got %s", testAwsSessionToken, credentials.SessionToken)
		}
	})

	t.Run("default_shared", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpFile := tmpDir + "/credentials"
		data := fmt.Sprintf("[%s]\naws_access_key_id = %s\naws_secret_access_key = %s\n", testAwsProfile, testAwsAccessKeyId, testAwsSecretAccessKey)
		if err := os.WriteFile(tmpFile, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AWS_SHARED_CREDENTIALS_FILE", tmpFile)
		t.Setenv("AWS_PROFILE", testAwsProfile)

		dest := &S3DestinationDefinition{
			Bucket: "example-bucket",
			Region: "eu-south-1",
		}

		client := dest.Client()
		if client == nil {
			t.Fatalf("expected client to be created")
		}
		if *client.Config.Region != dest.Region {
			t.Errorf("expected region %s, got %s", dest.Region, *client.Config.Region)
		}

		credentials, err := client.Config.Credentials.Get()
		if err != nil {
			t.Fatalf("expected credentials to be set, got %s", err)
		}
		if credentials.AccessKeyID != testAwsAccessKeyId {
			t.Errorf("expected access key id %s, got %s", testAwsAccessKeyId, credentials.AccessKeyID)
		}
		if credentials.SecretAccessKey != testAwsSecretAccessKey {
			t.Errorf("expected secret access key %s, got %s", testAwsSecretAccessKey, credentials.SecretAccessKey)
		}
		if credentials.SessionToken != "" {
			t.Errorf("expected session token %s, got %s", "", credentials.SessionToken)
		}
	})
}

func TestKey(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected, prefix, suffix string
		timestamp                time.Time
	}
	cases := map[string]testCase{
		"prefix_suffix": {
			expected:  "foo/20211008131625-bar.sql",
			prefix:    "foo/",
			suffix:    "-bar.sql",
			timestamp: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
		},
		"empty_prefix": {
			expected:  "20211008131625.sql",
			prefix:    "",
			suffix:    ".sql",
			timestamp: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			dest := S3DestinationDefinition{
				Prefix: testCase.prefix,
				Suffix: testCase.suffix,
			}
			actual := dest.Key(testCase.timestamp)
			if testCase.expected != actual {
				t.Errorf("expected %s, got %s", testCase.expected, actual)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected            time.Time
		prefix, suffix, key string
	}
	cases := map[string]testCase{
		"prefix_suffix": {
			expected: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
			prefix:   "foo/",
			suffix:   "-bar.sql",
			key:      "foo/20211008131625-bar.sql",
		},
		"empty_prefix": {
			expected: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
			prefix:   "",
			suffix:   ".sql",
			key:      "20211008131625.sql",
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			dest := S3DestinationDefinition{
				Prefix: testCase.prefix,
				Suffix: testCase.suffix,
			}

			if actual, err := dest.ParseTimestamp(testCase.key); err != nil {
				t.Errorf("unexpected error: %s", err)
			} else if !actual.Equal(testCase.expected) {
				t.Errorf("expected %s, got %s", testCase.expected, actual)
			}
		})
	}

	type errorCase struct {
		prefix, suffix, key string
	}
	errorCases := map[string]errorCase{
		"invalid_prefix": {
			prefix: "foo/",
			suffix: "-bar.sql",
			key:    "bar/20211008131625-bar.sql",
		},
		"invalid_suffix": {
			prefix: "",
			suffix: ".sql",
			key:    "20211008131625.tar.gz",
		},
		"invalid_timestamp": {
			prefix: "",
			suffix: ".sql",
			key:    "invalid.sql",
		},
	}
	for name, testCase := range errorCases {
		t.Run(name, func(t *testing.T) {
			dest := S3DestinationDefinition{
				Prefix: testCase.prefix,
				Suffix: testCase.suffix,
			}

			if actual, err := dest.ParseTimestamp(testCase.key); err == nil {
				t.Errorf("expected error, got %s", actual)
			} else if !actual.IsZero() {
				t.Errorf("expected zero time, got %s", actual)
			}
		})
	}
}
