package config

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/chialab/streamlined-backup/utils"
)

func TestLoadConfigurationToml(t *testing.T) {
	t.Parallel()

	data := `
[backup_mysql_database]
command = ["/bin/sh", "-c", "mysqldump --single-transaction --column-statistics=0 --set-gtid-purged=off my_database | bzip2"]
    [[backup_mysql_database.destination]]
    type = "s3"
    schedule = "30 4 * * *"
        [backup_mysql_database.destination.s3]
        region = "eu-west-1"
        profile = "streamlined-backup-test"
        bucket = "example-bucket"
        prefix = "my_database/daily/"
        suffix = "-my_database.sql.bz2"

[my_tar_archive]
command = ["tar", "-cvjf-", "/path/to/files"]
	[[my_tar_archive.destination]]
	type = "s3"
	schedule = "30 4 * * *"
        [my_tar_archive.destination.s3]
        region = "eu-west-1"
        bucket = "example-bucket"
        prefix = "my_tar_archive/daily/"
        suffix = "-my_tar_archive.tar.bz2"
            [my_tar_archive.destination.s3.credentials]
            access_key_id = "AKIAIOSFODNN7EXAMPLE"
            secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
`
	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "config.toml")
	if err := os.WriteFile(filePath, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	schedule, err := utils.NewSchedule("30 4 * * *")
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]Task{
		"backup_mysql_database": {
			Command: []string{"/bin/sh", "-c", "mysqldump --single-transaction --column-statistics=0 --set-gtid-purged=off my_database | bzip2"},
			Destination: []Destination{
				Destination{
					Type:     S3Destination,
					Schedule: *schedule,
					S3: S3DestinationDefinition{
						Region:  "eu-west-1",
						Profile: &testAwsProfile,
						Bucket:  "example-bucket",
						Prefix:  "my_database/daily/",
						Suffix:  "-my_database.sql.bz2",
					},
				},
			},
		},
		"my_tar_archive": {
			Command: []string{"tar", "-cvjf-", "/path/to/files"},
			Destination: []Destination{
				Destination{
					Type:     S3Destination,
					Schedule: *schedule,
					S3: S3DestinationDefinition{
						Region: "eu-west-1",
						Credentials: &S3Credentials{
							AccessKeyId:     "AKIAIOSFODNN7EXAMPLE",
							SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
						},
						Bucket: "example-bucket",
						Prefix: "my_tar_archive/daily/",
						Suffix: "-my_tar_archive.tar.bz2",
					},
				},
			},
		},
	}

	config, err := LoadConfiguration(filePath)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if !reflect.DeepEqual(config, expected) {
		t.Errorf("expected %#v, got %#v", expected, config)
	}
}

func TestLoadConfigurationTomlError(t *testing.T) {
	t.Parallel()

	data := `
[backup_mysql_database]
schedule = "30 4 * * *"
schedule = "31 4 * * *" # Duplicate key
`
	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "config.toml")
	if err := os.WriteFile(filePath, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	if config, err := LoadConfiguration(filePath); err == nil {
		t.Errorf("expected error, got nil")
	} else if config != nil {
		t.Errorf("expected nil, got %#v", config)
	} else if parseErr := new(toml.ParseError); !errors.As(err, parseErr) {
		t.Errorf("expected %T, got %T", parseErr, err)
	}
}

func TestLoadConfigurationJson(t *testing.T) {
	t.Parallel()

	data := `
{
"backup_mysql_database": {
    "command": ["/bin/sh", "-c", "mysqldump --single-transaction --column-statistics=0 --set-gtid-purged=off my_database | bzip2"],
    "destinations": [{
        "type": "s3",
        "schedule": "30 4 * * *",
        "s3": {
            "region": "eu-west-1",
            "profile": "streamlined-backup-test",
            "bucket": "example-bucket",
            "prefix": "my_database/daily/",
            "suffix": "-my_database.sql.bz2"
        }
    }]
},
"my_tar_archive": {
    "command": ["tar", "-cvjf-", "/path/to/files"],
    "destinations": [{
        "type": "s3",
        "schedule": "30 4 * * *",
        "s3": {
            "region": "eu-west-1",
            "bucket": "example-bucket",
            "prefix": "my_tar_archive/daily/",
            "suffix": "-my_tar_archive.tar.bz2",
            "credentials": {
                "access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	    }
	}
    }]
}
}
`
	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "config.json")
	if err := os.WriteFile(filePath, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	schedule, err := utils.NewSchedule("30 4 * * *")
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]Task{
		"backup_mysql_database": {
			Command: []string{"/bin/sh", "-c", "mysqldump --single-transaction --column-statistics=0 --set-gtid-purged=off my_database | bzip2"},
			Destination: []Destination{
				Destination{
					Type:     S3Destination,
					Schedule: *schedule,
					S3: S3DestinationDefinition{
						Region:  "eu-west-1",
						Profile: &testAwsProfile,
						Bucket:  "example-bucket",
						Prefix:  "my_database/daily/",
						Suffix:  "-my_database.sql.bz2",
					},
				},
			},
		},
		"my_tar_archive": {
			Command: []string{"tar", "-cvjf-", "/path/to/files"},
			Destination: []Destination{
				Destination{
					Type:     S3Destination,
					Schedule: *schedule,
					S3: S3DestinationDefinition{
						Region: "eu-west-1",
						Credentials: &S3Credentials{
							AccessKeyId:     "AKIAIOSFODNN7EXAMPLE",
							SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
						},
						Bucket: "example-bucket",
						Prefix: "my_tar_archive/daily/",
						Suffix: "-my_tar_archive.tar.bz2",
					},
				},
			},
		},
	}

	config, err := LoadConfiguration(filePath)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if !reflect.DeepEqual(config, expected) {
		t.Errorf("expected %#v, got %#v", expected, config)
	}
}

func TestLoadConfigurationJsonError(t *testing.T) {
	t.Parallel()

	data := `not a json`
	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "config.json")
	if err := os.WriteFile(filePath, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}

	if config, err := LoadConfiguration(filePath); err == nil {
		t.Errorf("expected error, got nil")
	} else if config != nil {
		t.Errorf("expected nil, got %#v", config)
	} else if parseErr := new(json.SyntaxError); !errors.As(err, &parseErr) {
		t.Errorf("expected %T, got %T", parseErr, err)
	}
}

func TestLoadConfigurationJsonMissingFileError(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "config.json")

	if config, err := LoadConfiguration(filePath); err == nil {
		t.Errorf("expected error, got nil")
	} else if config != nil {
		t.Errorf("expected nil, got %#v", config)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected %#v, got %#v", os.ErrNotExist, err)
	}
}

func TestLoadConfigurationInvalidFormatError(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "config.xml")

	if config, err := LoadConfiguration(filePath); err == nil {
		t.Errorf("expected error, got nil")
	} else if config != nil {
		t.Errorf("expected nil, got %#v", config)
	} else if !errors.Is(err, ErrUnsupportedConfigFile) {
		t.Errorf("expected %#v, got %#v", ErrUnsupportedConfigFile, err)
	}
}
