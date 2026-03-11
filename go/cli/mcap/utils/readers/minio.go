package readers

import (
	"context"
	"fmt"
	"io"
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

// tunedHTTPClient returns a client tuned for high-throughput S3-compatible storage:
//   - 4 MB socket read buffers  (vs Go default 4 KB)
//   - 100 idle conns per host   (allows reuse across parallel ops like merge)
//   - Expect-100 disabled       (saves one RTT per request)
//   - HTTP/2 disabled           (most S3-compatible endpoints advertise h2
//     via ALPN but don't implement it reliably)
func tunedHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          200,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 0,
			DisableCompression:    true,
			ReadBufferSize:        4 << 20,
			WriteBufferSize:       1 << 20,
		},
	}
}

// newS3CompatReader handles s3c://bucket/path/to/file.mcap.
//
// Required env vars:
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
		return nil, nil, fmt.Errorf("s3c: S3_ACCESS_KEY and S3_SECRET_KEY must be set")
	}

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
		return nil, nil, fmt.Errorf("s3c: failed to build config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	rs, err := NewS3ReadSeekCloser(ctx, client, actualBucket, key)
	if err != nil {
		return nil, nil, fmt.Errorf("s3c: failed to open s3c://%s/%s: %w", actualBucket, key, err)
	}

	// BufferedReadSeekCloser issues 32 MB range requests instead of the many
	// small ones that MCAP's chunk-by-chunk reader pattern would generate.
	// 900 MCAP chunks / minute → without buffer: 900 HTTP requests.
	// With 32 MB buffer covering ~40 chunks each: ~23 HTTP requests.
	brs := NewBufferedReadSeekCloser(rs, rs.size)
	return brs.Close, brs, nil
}
