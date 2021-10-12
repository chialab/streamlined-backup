package backup

type DestinationType string

const (
	S3Destination DestinationType = "s3"
)

type Destination struct {
	Type DestinationType
	S3   S3DestinationDefinition
}
