package utils

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct {
	*sqlx.DB
}

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 port=5433 user=dev_user dbname=transaction_db password=Dev$54321")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(500)
	db.SetMaxIdleConns(20000)
	db.SetConnMaxLifetime(60 * time.Minute)

	d := &DB{
		db,
	}
	return d, nil
}

func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		// Check for ZipSlip (Directory traversal)
		if !strings.HasPrefix(path, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", path)
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func ClearFile(specUrl string) error {
	absPath1, _ := filepath.Abs("../short_sale/extract/" + specUrl + ".txt")
	absPath2, _ := filepath.Abs("../short_sale/" + specUrl + ".zip")

	e := os.Remove(absPath1)
	if e != nil {
		log.Println(e)
	}
	e = os.Remove(absPath2)
	if e != nil {
		log.Println(e)
	}
	return e
}
