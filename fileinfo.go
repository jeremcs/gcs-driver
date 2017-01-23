package gcsdriver

import (
	"os"
	"time"

    "github.com/lunny/log"
    storage "google.golang.org/api/storage/v1"
)

type FileInfo struct {
	name  string
	isDir bool
    User string
	Object storage.Object
}

func (f *FileInfo) Name() string {
	return f.name
}

func (f *FileInfo) Size() int64 {
	return (int64)(f.Object.Size)
}

func (f *FileInfo) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | os.ModePerm
	}
	return os.ModePerm
}

func (f *FileInfo) ModTime() time.Time {
    parsed, err := time.Parse(time.RFC3339, f.Object.Updated)
    if err != nil {
        log.Error("Could not parse time for string " + f.Object.Updated)
        return time.Now()
    }
	return parsed
}

func (f *FileInfo) IsDir() bool {
	return f.isDir
}

func (f *FileInfo) Sys() interface{} {
	return nil
}

func (f *FileInfo) Owner() string {
    return f.User
}

func (f *FileInfo) Group() string {
	return f.User
}
