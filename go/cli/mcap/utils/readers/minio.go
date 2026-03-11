package readers

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func init() {
	RegisterReader("s3c", newS3CompatReader)
}

// newS3CompatReader handles s3c://bucket/path/to/file.mcap using S3_* env vars.
// S3_ENDPOINT  - required, e.g. https://ca1.obj.nexgencloud.io
// S3_ACCESS_KEY - required
// S3_SECRET_KEY - required
func newS3CompatReader(ctx context.Context, bucket, objectPath string) (func() error, io.ReadSeekCloser, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")

	if endpoint == "" {
		return nil, nil, fmt.Errorf("s3c: S3_ENDPOINT env var is not set")
	}
	if accessKey == "" || secretKey == "" {
		return nil, nil, fmt.Errorf("s3c: S3_ACCESS_KEY and S3_SECRET_KEY env vars must be set")
	}

	// objectPath may include a leading bucket segment if bucket was empty — handle both:
	// s3c://mybucket/key/file.mcap  → bucket=mybucket, objectPath=key/file.mcap
	// s3c:///mybucket/key/file.mcap → bucket="", objectPath=mybucket/key/file.mcap
	actualBucket := bucket
	key := objectPath
	if actualBucket == "" {
		parts := strings.SplitN(strings.TrimPrefix(objectPath, "/"), "/", 2)
		if len(parts) < 2 {
			return nil, nil, fmt.Errorf("s3c: path must be bucket/key, got: %q", objectPath)
		}
		actualBucket, key = parts[0], parts[1]
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"), // S3-compatible providers ignore region
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("s3c: failed to build AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // required for most S3-compatible providers
	})

	rs, err := NewS3ReadSeekCloser(ctx, client, actualBucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("s3c: failed to open object s3c://%s/%s: %w", actualBucket, key, err)
	}
	return rs.Close, rs, nil
}
