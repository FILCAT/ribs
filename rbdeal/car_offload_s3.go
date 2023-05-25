package rbdeal

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	pool "github.com/libp2p/go-buffer-pool"
	"go.uber.org/multierr"
	"io"
	"net/url"
	"os"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/xerrors"

	iface "github.com/lotus-web3/ribs"
)

func (r *ribs) maybeInitS3Offload() error {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		return nil
	}

	log.Errorw("S3 offload enabled", "endpoint", endpoint)

	region := os.Getenv("S3_REGION")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	token := os.Getenv("S3_TOKEN")
	bucket := os.Getenv("S3_BUCKET")
	bucketUrl := os.Getenv("S3_BUCKET_URL")

	burl, err := url.Parse(bucketUrl)
	if err != nil {
		return xerrors.Errorf("failed to parse S3_BUCKET_URL: %w", err)
	}

	s3Config := &aws.Config{
		Endpoint:    aws.String(endpoint),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, token),
		Region:      aws.String(region),
	}

	asess, err := session.NewSession(s3Config)
	if err != nil {
		return xerrors.Errorf("failed to create S3 session: %w", err)
	}

	r.s3 = s3.New(asess)
	r.s3Bucket = bucket
	r.s3BucketUrl = burl

	return nil
}

func (r *ribs) maybeEnsureS3Offload(gid iface.GroupKey) error {
	if r.s3 == nil {
		return nil
	}

	has, err := r.db.HasS3Offload(gid)
	if err != nil {
		return xerrors.Errorf("failed to check if group %d has S3 offload: %w", gid, err)
	}
	if has {
		return nil
	}

	// check if already uploaded
	r.s3Lk.Lock()
	_, ok := r.s3Uploads[gid]
	if ok {
		r.s3Lk.Unlock()
		return xerrors.Errorf("group %d has an ongoing upload", gid)
	}

	has, err = r.db.HasS3Offload(gid)
	if err != nil {
		return xerrors.Errorf("failed to check if group %d has S3 offload: %w", gid, err)
	}
	if has {
		return nil
	}

	r.s3Uploads[gid] = struct{}{}

	defer func() {
		r.s3Lk.Lock()
		delete(r.s3Uploads, gid)
		r.s3Lk.Unlock()
	}()
	r.s3Lk.Unlock()

	ctx := context.TODO()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr, pw := io.Pipe()

	syncWait := make(chan struct{})
	go func() {
		defer close(syncWait)

		bw := bufio.NewWriterSize(pw, 4<<20)

		err := r.RBS.Storage().ReadCar(ctx, gid, bw)
		if err != nil {
			perr := pw.CloseWithError(err)
			if perr != nil {
				log.Errorw("failed to close pipe", "error", perr)
			}
		}

		perr := pw.CloseWithError(bw.Flush())
		if perr != nil {
			log.Errorw("failed to close pipe", "error", perr)
		}
	}()

	upErr := r.uploadGroupData(gid, pr)
	if upErr != nil {
		return xerrors.Errorf("failed to upload group %d: %w", gid, upErr)
	}

	perr := pr.Close()

	<-syncWait

	if perr != nil {
		return xerrors.Errorf("failed to close pipe (read): %w", perr)
	}

	if err := r.db.AddS3Offload(gid); err != nil {
		return xerrors.Errorf("noting s3 offload: %w", err)
	}

	return nil
}

func (r *ribs) maybeGetS3URL(gid iface.GroupKey) (string, error) {
	has, err := r.db.HasS3Offload(gid)
	if err != nil {
		return "", xerrors.Errorf("failed to check if group %d has S3 offload: %w", gid, err)
	}

	if !has {
		return "", nil
	}

	urlCopy := *r.s3BucketUrl
	urlCopy.Path = path.Join(urlCopy.Path, fmt.Sprintf("gdata%d.car", gid))

	return urlCopy.String(), nil
}

var partSize = 64 << 20 // todo investigate streaming much larger parts

func (r *ribs) uploadGroupData(gid iface.GroupKey, src io.Reader) error {
	objKey := fmt.Sprintf("gdata%d.car", gid)

	createResp, err := r.s3.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: &r.s3Bucket,
		Key:    &objKey,
	})
	if err != nil {
		return xerrors.Errorf("failed to create multipart upload: %w", err)
	}

	uploadId := *createResp.UploadId
	var partsLk sync.Mutex
	var completedParts []*s3.CompletedPart
	var errors []error

	partNumber := int64(1)

	maxParallel := 8 // todo: make this configurable

	throttle := make(chan struct{}, maxParallel)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	for {
		part := pool.Get(partSize)
		n, err := io.ReadFull(src, part)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return xerrors.Errorf("failed to read part: %w", err)
		}
		if n == 0 {
			break
		}

		throttle <- struct{}{}

		go func(part []byte, n int, partNumber int64) {
			defer func() {
				<-throttle

				pool.Put(part)
			}()

			uploadResp, err := r.s3.UploadPartWithContext(ctx, &s3.UploadPartInput{
				Body:       bytes.NewReader(part[:n]),
				Bucket:     &r.s3Bucket,
				Key:        &objKey,
				PartNumber: aws.Int64(partNumber),
				UploadId:   &uploadId,
			})

			partsLk.Lock()
			if err != nil {
				errors = append(errors, xerrors.Errorf("failed to upload part %d: %w", partNumber, err))
			} else {
				completedParts = append(completedParts, &s3.CompletedPart{
					ETag:       uploadResp.ETag,
					PartNumber: aws.Int64(partNumber),
				})
			}
			partsLk.Unlock()

			log.Errorw("uploaded part", "part", partNumber, "group", gid, "size", n)
		}(part, n, partNumber)

		partNumber++
	}

	for i := 0; i < maxParallel; i++ {
		throttle <- struct{}{}
	}

	if len(errors) > 0 {
		return xerrors.Errorf("failed to upload parts: %w", multierr.Combine(errors...))
	}

	// Complete the multipart upload
	_, err = r.s3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   &r.s3Bucket,
		Key:      &objKey,
		UploadId: &uploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return xerrors.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

func (r *ribs) cleanupS3Offload(gid iface.GroupKey) error {
	has, err := r.db.HasS3Offload(gid)
	if err != nil {
		return xerrors.Errorf("failed to check if group %d has S3 offload: %w", gid, err)
	}

	if !has {
		return nil
	}

	objKey := fmt.Sprintf("gdata%d.car", gid)

	_, err = r.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &r.s3Bucket,
		Key:    &objKey,
	})

	if err != nil {
		return xerrors.Errorf("failed to delete object: %w", err)
	}

	if err := r.db.DropS3Offload(gid); err != nil {
		return xerrors.Errorf("failed to remove s3 offload: %w", err)
	}

	return nil
}
