package s3remote

import (
	"io"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3RemoteStore struct {
	Bucket string
	Root   string
	client *s3.S3
}

func (store *S3RemoteStore) GetMeta(name string) (stream io.ReadCloser, size int64, err error) {
	key := path.Join(store.Root, name)
	input_ := &s3.GetObjectInput{
		Bucket: &store.Bucket,
		Key:    &key,
	}
	output_, err := store.client.GetObject(input_)
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
	output_, err := store.client.GetObject(input_)
	if err != nil {
		return nil, 0, err
	}
	return output_.Body, *output_.ContentLength, nil
}

func New(bucket string, rootKey string, cfgs ...*aws.Config) (store *S3RemoteStore, err error) {
	sess, err := session.NewSession(cfgs...)
	if err != nil {
		return nil, err
	}
	client := s3.New(sess)
	return &S3RemoteStore{
		Bucket: bucket,
		Root:   rootKey,
		client: client,
	}, nil
}
