package readers

import (
	"context"
	"fmt"
	"io"
	"crypto/tls"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func init() {
	RegisterReader("s3c", newS3CompatReader)
}

// tunedHTTPClient returns an HTTP client optimised for high-throughput
// S3-compatible object storage:
//   - 4 MB read/write socket buffers (vs Go default 4 KB)
//   - HTTP/2 enabled (multiplexing, no head-of-line blocking)
//   - Expect: 100-continue disabled (saves a full RTT per PUT)
//   - 100 idle conns per host (allows concurrent range requests)
//   - Compression disabled (we stream pre-compressed MCAP/zstd data)
func tunedHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          200,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 0,    // disable Expect: 100-continue
			DisableCompression:    true, // data is already compressed
			ReadBufferSize:        4 << 20,
			WriteBufferSize:       1 << 20,
			ForceAttemptHTTP2:     false,
			// Explicitly disable HTTP/2 — most S3-compatible endpoints
			// advertise h2 via ALPN but don't implement it correctly.
			TLSNextProto: make(map[string]func(string, *tls.Conn) http.RoundTripper),
		},
	}
}

// newS3CompatReader handles s3c://bucket/path/to/file.mcap using S3_* env vars.
//
// Required environment variables:
//
//	S3_ENDPOINT   e.g. https://ca1.obj.nexgencloud.io
//	S3_ACCESS_KEY
//	S3_SECRET_KEY
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

	// objectPath may include a leading bucket if bucket was empty — handle both:
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
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
		config.WithHTTPClient(tunedHTTPClient()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("s3c: failed to build AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // required for MinIO / most S3-compatible providers
	})

	rs, err := NewS3ReadSeekCloser(ctx, client, actualBucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("s3c: failed to open object s3c://%s/%s: %w", actualBucket, key, err)
	}

	// Wrap with buffered reader: issues 64 MB range requests instead of many
	// small ones, dramatically reducing round trips on S3-compatible storage.
	brs := NewBufferedReadSeekCloser(rs, rs.size)
	return brs.Close, brs, nil
}
