package s3remote

import (
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3RemoteStore struct {
	Bucket string
	Root   string
	client *s3.S3
	cfg    *aws.Config
}

func (store *S3RemoteStore) GetMeta(name string) (stream io.ReadCloser, size int64, err error) {
	key := path.Join(store.Root, name)
	input_ := &s3.GetObjectInput{
		Bucket: &store.Bucket,
		Key:    &key,
	}

	req := store.client.GetObjectRequest(input_)
	output_, err := req.Send()
	if err != nil {
		return
	}
	return output_.Body, *output_.ContentLength, nil
}

func (store *S3RemoteStore) GetTarget(name string) (stream io.ReadCloser, size int64, err error) {
	key := path.Join(store.Root, "targets", name)
	input_ := &s3.GetObjectInput{
		Bucket: &store.Bucket,
		Key:    &key,
	}
	req := store.client.GetObjectRequest(input_)
	output_, err := req.Send()
	if err != nil {
		return nil, 0, err
	}
	return output_.Body, *output_.ContentLength, nil
}

func New(bucket string, rootKey string, cfg *aws.Config) (store *S3RemoteStore, err error) {
	if cfg == nil {
		newConfig, err := external.LoadDefaultAWSConfig()
		if err != nil {
			return nil, err
		}
		cfg = &newConfig
	}
	client := s3.New(*cfg)
	return &S3RemoteStore{
		Bucket: bucket,
		Root:   rootKey,
		client: client,
		cfg:    cfg,
	}, nil
}
