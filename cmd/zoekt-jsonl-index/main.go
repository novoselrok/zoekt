package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/sourcegraph/zoekt/build"
)

func main() {
	indexDir := flag.String("index-dir", build.DefaultDir, "index directory for *.zoekt files")
	filePath := flag.String("file", "", "file to index (gzipped jsonl)")
	dir := flag.String("dir", "", "dir to index (has to contain gzipped jsonl files)")
	flag.Parse()

	ctx := context.Background()

	opts := build.Options{IndexDir: *indexDir}
	opts.RepositoryDescription.Name = "magicsearchdev"
	builder, err := build.NewBuilder(opts)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = builder.Finish()
		if err != nil {
			log.Fatal(err)
		}
	}()

	if *filePath != "" {
		log.Printf("Indexing %s", *filePath)

		err = indexFile(ctx, builder, *filePath)
		if err != nil {
			log.Fatal(err)
		}
	} else if *dir != "" {
		paths, err := listFiles(ctx, *dir, ".jsonl.gz")
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Indexing %d files from dir %s", len(paths), *dir)

		for _, path := range paths {
			err = indexFile(ctx, builder, path)
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		log.Fatal("Missing -file or -dir option.")
	}
}

func indexFile(ctx context.Context, builder *build.Builder, path string) error {
	fileEntries, err := fileEntriesFromPath(ctx, path)
	if err != nil {
		return err
	}

	for _, entry := range fileEntries {
		if err := builder.AddFile(fmt.Sprintf("%s/%s", entry.Repository, entry.FilePath), []byte(entry.Content)); err != nil {
			return err
		}
	}

	return nil
}
