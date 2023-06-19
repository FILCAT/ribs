package rbdeal

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	pool "github.com/libp2p/go-buffer-pool"
	"go.uber.org/multierr"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"sync"
	"time"

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

	r.RBS.StagingStorage().InstallStagingProvider(&ribsStagingProvider{r: r})

	return nil
}

func (r *ribs) maybeEnsureS3Offload(gid iface.GroupKey) error {
	if r.s3 == nil {
		return nil
	}

	return r.maybeDoS3OffloadWithSource(gid, r.RBS.Storage().ReadCar)
}

func (r *ribs) maybeDoS3OffloadWithSource(gid iface.GroupKey, source func(ctx context.Context, group iface.GroupKey, out io.Writer) error) error {
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

		err := source(ctx, gid, bw)
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

var partSize = 128 << 20 // todo investigate streaming much larger parts
var minPartSize = 8 << 20

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

	maxParallel := 4 // todo: make this configurable

	throttle := make(chan struct{}, maxParallel)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	curPart := pool.Get(partSize)
	n, err := io.ReadFull(src, curPart)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return xerrors.Errorf("failed to read part: %w", err)
	}
	curPart = curPart[:n]

	for {
		if len(curPart) == 0 {
			break
		}

		// read next part now, so that in case it's smaller than 5MB, we can merge it with the current one
		nextPart := pool.Get(partSize)
		n, err := io.ReadFull(src, nextPart)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return xerrors.Errorf("failed to read part: %w", err)
		}
		nextPart = nextPart[:n]

		if len(nextPart) < minPartSize && len(nextPart) > 0 {
			// last part is too small, merge it with the current one
			log.Errorw("last part merge", "cur", len(curPart), "next", len(nextPart), "total", len(curPart)+len(nextPart))

			temp := pool.Get(len(curPart) + len(nextPart))

			cn := copy(temp, curPart)

			if cn != len(curPart) {
				return xerrors.Errorf("failed to copy part: %d != %d", cn, len(curPart))
			}

			pool.Put(curPart)

			copy(temp[cn:], nextPart)
			pool.Put(nextPart)

			curPart = temp
			nextPart = []byte{}
		}

		throttle <- struct{}{}

		go func(part []byte, partNumber int64) {
			defer func() {
				<-throttle

				pool.Put(part)
			}()

			maxRetries := 6
			for i := 0; i < maxRetries; i++ {
				uploadResp, err := r.s3.UploadPartWithContext(ctx, &s3.UploadPartInput{
					Body:       bytes.NewReader(part),
					Bucket:     &r.s3Bucket,
					Key:        &objKey,
					PartNumber: aws.Int64(partNumber),
					UploadId:   &uploadId,
				})

				partsLk.Lock()
				if err != nil {
					// If we've reached the maximum retries, append the error
					if i == maxRetries-1 {
						errors = append(errors, xerrors.Errorf("failed to upload part %d: %w", partNumber, err))
					}
					log.Errorw("failed to upload part", "part", partNumber, "group", gid, "error", err)
					time.Sleep(time.Second << uint(i))
				} else {
					completedParts = append(completedParts, &s3.CompletedPart{
						ETag:       uploadResp.ETag,
						PartNumber: aws.Int64(partNumber),
					})
					partsLk.Unlock()
					log.Errorw("uploaded part", "part", partNumber, "group", gid, "size", len(part))
					break
				}
				partsLk.Unlock()
			}
		}(curPart, partNumber)

		curPart = nextPart

		partNumber++
	}

	// wait for all upload goroutines to finish
	for i := 0; i < maxParallel; i++ {
		throttle <- struct{}{}
	}

	if len(errors) > 0 {
		// remove failed upload
		_, err2 := r.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   &r.s3Bucket,
			Key:      &objKey,
			UploadId: &uploadId,
		})
		if err2 != nil {
			log.Errorw("failed to abort multipart upload", "group", gid, "err", err2, "completeErr", err)
		}

		return xerrors.Errorf("failed to upload parts: %w", multierr.Combine(errors...))
	}

	// sort completed parts by part number
	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

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
		// remove failed upload
		_, err2 := r.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   &r.s3Bucket,
			Key:      &objKey,
			UploadId: &uploadId,
		})
		if err2 != nil {
			log.Errorw("failed to abort multipart upload", "group", gid, "err", err2, "completeErr", err)
		}

		log.Errorw("failed to complete multipart upload", "group", gid, "err", err, "parts", completedParts)

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

type ribsStagingProvider struct {
	r *ribs
}

func (r *ribsStagingProvider) Upload(ctx context.Context, group iface.GroupKey, src func(writer io.Writer) error) error {
	return r.r.maybeDoS3OffloadWithSource(group, func(ctx context.Context, group iface.GroupKey, out io.Writer) error {
		return src(out)
	})
}

func (r *ribsStagingProvider) ReadCar(ctx context.Context, group iface.GroupKey, off, size int64) (io.ReadCloser, error) {
	u, err := r.URL(ctx, group)
	if err != nil {
		return nil, xerrors.Errorf("get url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, xerrors.Errorf("new request: %w", err)
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", off, off+size-1))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("perform request: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, xerrors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return resp.Body, nil
}
func (r *ribsStagingProvider) URL(ctx context.Context, group iface.GroupKey) (string, error) {
	return r.r.maybeGetS3URL(group)
}
