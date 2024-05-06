package gminio

import (
	"bytes"
	miniocore "github.com/myfstd/gminio/core"
	"log"
	"os"
	"time"
)

type Client struct {
	client *miniocore.Client
	bucket string
}

// NewClient 初始化minio
func NewClient(endpoint string, accessKey string, secretKey string, bucketName string) (*Client, error) {
	// 初始化 Minio 客户端
	minioClient, err := miniocore.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		log.Println("new minio client fail: ", err)
		return nil, err
	}
	exists, err := minioClient.BucketExists(bucketName)
	if err != nil {
		log.Println("get minio bucket fail: ", err)
		return nil, err
	}
	if !exists {
		err = minioClient.MakeBucket(bucketName, "local_region")
		if err != nil {
			log.Println("create minio bucket fail: ", err)
			return nil, err
		}
	}
	client := &Client{
		client: minioClient,
		bucket: bucketName,
	}
	return client, nil
}

// UploadFile 通过路径上传文件
func (m *Client) UploadFile(objectName string, filePath string) error {
	// 打开本地文件
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("open filePath: %s fail: %s", filePath, err)
		return err
	}
	defer file.Close()

	// 上传文件到存储桶
	_, err = m.client.PutObject(m.bucket, objectName, file, -1, miniocore.PutObjectOptions{})
	if err != nil {
		log.Println("putObject fail: ", err)
		return err
	}
	return nil
}

// PutFile 通过文件上传
func (m *Client) PutFile(objectName string, file *os.File) error {
	_, err := m.client.PutObject(m.bucket, objectName, file, -1, miniocore.PutObjectOptions{})
	if err != nil {
		log.Println("objectName fail: ", err)
		return err
	}
	return nil
}

// PutBytes 通过文件流上传
func (m *Client) PutBytes(objectName string, data []byte) error {
	_, err := m.client.PutObject(m.bucket, objectName, bytes.NewReader(data), -1, miniocore.PutObjectOptions{})
	if err != nil {
		log.Println("PutBytes fail: ", err)
		return err
	}
	return nil
}

// DownloadFile 下载文件
func (m *Client) DownloadFile(objectName string, filePath string) error {
	// 创建本地文件
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 下载存储桶中的文件到本地
	err = m.client.FGetObject(m.bucket, objectName, filePath, miniocore.GetObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

// DeleteFile 删除文件
func (m *Client) DeleteFile(objectName string) (bool, error) {
	// 删除存储桶中的文件
	err := m.client.RemoveObject(m.bucket, objectName)
	if err != nil {
		log.Println("remove object fail: ", err)
		return false, err
	}
	return true, err
}

// ListObjects 列出文件
func (m *Client) ListObjects(prefix string) ([]string, error) {
	var objectNames []string

	for object := range m.client.ListObjects(m.bucket, prefix, true, nil) {
		if object.Err != nil {
			return nil, object.Err
		}

		objectNames = append(objectNames, object.Key)
	}

	return objectNames, nil
}

// GetObjectUrl 返回对象的url地址，有效期时间为expires
func (m *Client) GetObjectUrl(objectName string, expires ...time.Duration) (string, error) {
	expiresTime := 24 * time.Hour
	if expires != nil {
		expiresTime = expires[0]
	}
	object, err := m.client.PresignedGetObject(m.bucket, objectName, expiresTime, nil)
	if err != nil {
		log.Println("get object fail: ", err)
		return "", err
	}

	return object.String(), nil
}
