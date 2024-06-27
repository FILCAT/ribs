package rbdeal

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	"go.uber.org/multierr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/xerrors"

	iface "github.com/atboosty/ribs"
)

func (r *ribs) maybeInitS3Offload() error {
	need, err := r.db.NeedS3Offload()
	if err != nil {
		return xerrors.Errorf("failed to check if S3 offload is needed: %w", err)
	}

	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		if need {
			return xerrors.Errorf("S3 offload enabled but S3_ENDPOINT not set")
		}

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

func (r *ribs) maybeDoS3OffloadWithSource(gid iface.GroupKey, source func(ctx context.Context, group iface.GroupKey, sz func(int64), out io.Writer) error) error {
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

	sizeCh := make(chan int64, 1)
	setSize := func(sz int64) {
		sizeCh <- sz
		close(sizeCh)
	}

	syncWait := make(chan struct{})
	go func() {
		defer close(syncWait)

		bw := bufio.NewWriterSize(pw, 4<<20)

		err := source(ctx, gid, setSize, bw)
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

	size := <-sizeCh

	upErr := r.uploadGroupData(gid, size, pr)
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

const partSize = 128 << 20 // todo investigate streaming much larger parts
const minPartSize = 8 << 20
const adjustmentSize = 100 << 10 // 100 KiB

func CalculateChunkSize(fileSize int64) int {
	numParts := int64(math.Ceil(float64(fileSize) / float64(partSize)))
	chunkSize := int64(partSize)

	lastPartSize := fileSize - ((numParts - 1) * chunkSize)

	// Ensure the last part is at least 5 MiB and not larger than chunk size
	for lastPartSize < minPartSize || lastPartSize > chunkSize {
		// Decrease the chunk size if last part size is too small
		// Or increase it if last part size is too large
		if lastPartSize < minPartSize {
			chunkSize -= adjustmentSize
		} else if lastPartSize > chunkSize {
			chunkSize += adjustmentSize
		}

		numParts = int64(math.Ceil(float64(fileSize) / float64(chunkSize)))
		lastPartSize = fileSize - ((numParts - 1) * chunkSize)
		log.Errorw("try chunk size", "chunkSize", chunkSize, "lastPartSize", lastPartSize, "numParts", numParts, "fileSize", fileSize)
	}

	log.Errorw("using chunk size", "chunkSize", chunkSize, "lastPartSize", lastPartSize, "numParts", numParts, "fileSize", fileSize)

	return int(chunkSize)
}

func (r *ribs) uploadGroupData(gid iface.GroupKey, size int64, src io.Reader) (err error) {
	r.s3UploadStarted.Add(1)
	defer func() {
		if err != nil {
			r.s3UploadDone.Add(1)
		} else {
			r.s3UploadErr.Add(1)
		}
	}()

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

	partSize := CalculateChunkSize(size)

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

				r.s3UploadBytes.Add(int64(len(part)))

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

func (r *ribsStagingProvider) HasCar(ctx context.Context, group iface.GroupKey) (bool, error) {
	has, err := r.r.db.HasS3Offload(group)
	if err != nil {
		return false, xerrors.Errorf("failed to check if group %d has S3 offload: %w", group, err)
	}
	return has, nil
}

func (r *ribsStagingProvider) Upload(ctx context.Context, group iface.GroupKey, size int64, src func(writer io.Writer) error) error {
	return r.r.maybeDoS3OffloadWithSource(group, func(ctx context.Context, group iface.GroupKey, sz func(int642 int64), out io.Writer) error {
		sz(size)
		return src(out)
	})
}

func (r *ribsStagingProvider) ReadCar(ctx context.Context, group iface.GroupKey, off, size int64) (io.ReadCloser, error) {
	has, err := r.r.db.HasS3Offload(group)
	if err != nil {
		return nil, xerrors.Errorf("failed to check if group %d has S3 offload: %w", group, err)
	}
	if !has {
		return nil, xerrors.Errorf("group %d does not have S3 offload", group)
	}

	r.r.s3ReadReqs.Add(1)
	r.r.s3ReadBytes.Add(size)

	key := fmt.Sprintf("gdata%d.car", group)

	req, _ := r.r.s3.GetObjectRequest(&s3.GetObjectInput{
		Bucket: &r.r.s3Bucket,
		Key:    &key,
	})

	req.HTTPRequest.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", off, off+size-1))

	err = req.Send()

	if err != nil {
		return nil, xerrors.Errorf("failed to send request: %w", err)
	}

	return req.HTTPResponse.Body, nil
	/*
		///

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

		return resp.Body, nil*/
}

/*
func (r *ribsStagingProvider) URL(ctx context.Context, group iface.GroupKey) (string, error) {
	return r.r.maybeGetS3URL(group)
}*/

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
