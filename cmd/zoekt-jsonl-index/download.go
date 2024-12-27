package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type FileEntry struct {
	Content    string `json:"content"`
	Repository string `json:"repository"`
	FilePath   string `json:"file_path"`
}

func fileEntriesFromPath(ctx context.Context, path string) ([]FileEntry, error) {
	var reader io.ReadCloser
	var err error

	if strings.HasPrefix(path, "gs://") {
		parts := strings.SplitN(strings.TrimPrefix(path, "gs://"), "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid GCS path format, expected 'gs://bucket/object'")
		}
		bucketName, objectPath := parts[0], parts[1]

		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage client: %v", err)
		}
		defer client.Close()

		reader, err = client.Bucket(bucketName).Object(objectPath).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to open GCS object: %v", err)
		}
	} else {
		reader, err = os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("failed to open local file: %v", err)
		}
	}
	defer reader.Close()

	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decoder := json.NewDecoder(gzipReader)
	var entries []FileEntry
	for {
		var entry FileEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decode JSON: %v", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func listFiles(ctx context.Context, dirPath, suffix string) ([]string, error) {
	if strings.HasPrefix(dirPath, "gs://") {
		return listGCSFiles(ctx, dirPath, suffix)
	}
	return listLocalFiles(dirPath, suffix)
}

func listGCSFiles(ctx context.Context, gcsPath, suffix string) ([]string, error) {
	parts := strings.SplitN(strings.TrimPrefix(gcsPath, "gs://"), "/", 2)
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid GCS path format, expected 'gs://bucket/prefix'")
	}

	bucketName := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
		// Ensure prefix ends with / if it's not empty
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	var files []string

	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating over bucket: %v", err)
		}

		if strings.HasSuffix(attrs.Name, suffix) {
			files = append(files, fmt.Sprintf("gs://%s/%s", bucketName, attrs.Name))
		}
	}

	return files, nil
}

func listLocalFiles(dirPath, suffix string) ([]string, error) {
	var files []string

	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), suffix) {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %v", err)
	}

	return files, nil
}
