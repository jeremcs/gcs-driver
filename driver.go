package gcsdriver

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/goftp/server"
	"github.com/lunny/log"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	storage "google.golang.org/api/storage/v1"
)

var (
	scope = storage.DevstorageFullControlScope
)

// Can use a specific bucket, or one per user
type GoogleStorageDriver struct {
	curDir        string
	bucket        string
	bucketPerUser bool
	httpClient    *http.Client
	gcs           *storage.Service
	conn          *server.Conn
}

// This functions returns the json web token serialized in the file located at filepath
func getJWT(filepath string) *jwt.Config {
	file, err := os.Open(filepath)
	if err != nil {
		log.Error("Unable to open service account file", err)
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Error("Unable to read service account file", err)
	}

	token, err := google.JWTConfigFromJSON(bytes, scope)
	if err != nil {
		log.Error("Unable to parse service account file", err)
	}

	return token
}

// Returns an http.Client authenticated with the input JWT
func getClient(token *jwt.Config) *http.Client {
	return token.Client(context.Background())
}

// Returns a Google Storage service object using the provided http.Client
func getService(client *http.Client) *storage.Service {
	service, err := storage.New(client)
	if err != nil {
		log.Fatalf("Unable to create storage service: %v", err)
	}
	log.Info("Created storage service")
	return service

}

// Store a pointer to the connection to keep access to values like the logged in user
func (driver *GoogleStorageDriver) Init(conn *server.Conn) {
	driver.conn = conn
}

// Get the current user's bucket. Can be global or user-specific according to the driver's settings
func (driver *GoogleStorageDriver) getUserBucket() string {
	if driver.bucketPerUser {
		return driver.bucket + "-" + driver.conn.LoginUser()
	} else {
		return driver.bucket
	}
}

// Implementation of goftp's ChangeDir method for Google Storage
func (driver *GoogleStorageDriver) ChangeDir(path string) error {
	f, err := driver.Stat(path)
	if err != nil {
		return err
	}

	if !f.IsDir() {
		return errors.New("Not a directory")
	}
	driver.curDir = path
	return nil
}

// Implementation of goftp's Stat method for Google Storage
func (driver *GoogleStorageDriver) Stat(key string) (server.FileInfo, error) {
	user := driver.conn.LoginUser()

	if strings.HasSuffix(key, "/") {
		result := &FileInfo{
			name:   key,
			isDir:  true,
			User:   user,
			Object: storage.Object{},
		}
		return result, nil
	}

	entry, err := driver.gcs.Objects.Get(driver.getUserBucket(), strings.TrimLeft(key, "/")).Do()
	if err != nil {
		entries, err := driver.gcs.Objects.List(driver.getUserBucket()).Prefix(strings.TrimLeft(key, "/")).Do()
		if err != nil {
			log.Fatal("Could not list objects")
		}
		if len(entries.Items) > 0 {
			result := &FileInfo{
				name:   key,
				isDir:  true,
				User:   driver.conn.LoginUser(),
				Object: storage.Object{},
			}
			return result, nil
		}
		return nil, errors.New("This directory does not exist")
	}

	result := &FileInfo{
		name:   key,
		isDir:  false,
		User:   user,
		Object: *entry,
	}
	return result, nil
}

// Implementation of goftp's ListDir method for Google Storage
func (driver *GoogleStorageDriver) ListDir(prefix string, callback func(server.FileInfo) error) error {
	d := strings.TrimLeft(prefix, "/")
	if d != "" {
		d = d + "/"
	}
	entries, err := driver.gcs.Objects.List(driver.getUserBucket()).Prefix(d).Do()
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return err
	}

	dirCache := make(map[string]bool)

	for _, entry := range entries.Items {
		if prefix != "/" && prefix != "" && !strings.HasPrefix(entry.Name, d) {
			continue
		}
		key := strings.TrimLeft(strings.TrimLeft(entry.Name, d), "/")
		if key == "" {
			continue
		}
		var f *FileInfo
		if strings.Contains(key, "/") {
			key := strings.Trim(strings.Split(key, "/")[0], "/")
			if _, ok := dirCache[key]; ok {
				continue
			}
			dirCache[key] = true
			f = &FileInfo{
				name:   key,
				isDir:  true,
				Object: *entry,
				User:   driver.conn.LoginUser(),
			}
		} else {
			f = &FileInfo{
				name:   key,
				Object: *entry,
				User:   driver.conn.LoginUser(),
			}
		}
		err = callback(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// Implementation of goftp's DeleteDir method for Google Storage
func (driver *GoogleStorageDriver) DeleteDir(key string) error {
	d := strings.TrimLeft(key, "/")

	entries, err := driver.gcs.Objects.List(driver.getUserBucket()).Prefix(d).Do()
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return err
	}
	if len(entries.Items) == 0 {
		return nil
	}

	for _, entry := range entries.Items {
		err = driver.gcs.Objects.Delete(driver.getUserBucket(), entry.Name).Do()
		if err != nil {
			return err
		}
	}
	return nil
}

// Implementation of goftp's DeleteFile method for Google Storage
func (driver *GoogleStorageDriver) DeleteFile(key string) error {
	fmt.Println("delete file", key)
	return driver.gcs.Objects.Delete(driver.getUserBucket(), strings.TrimLeft(key, "/")).Do()
}

// Implementation of goftp's Rename method for Google Storage
func (driver *GoogleStorageDriver) Rename(keySrc, keyDest string) error {
	fmt.Println("rename from", keySrc, keyDest)
	var from = strings.TrimLeft(keySrc, "/")
	var to = strings.TrimLeft(keyDest, "/")
	_, err := driver.gcs.Objects.Get(driver.getUserBucket(), from).Do()
	if err != nil && strings.Contains(err.Error(), "no such file or directory") {
		from = strings.TrimLeft(keySrc, "/") + "/"
		to = strings.TrimLeft(keyDest, "/") + "/"
		_, err = driver.gcs.Objects.Get(driver.getUserBucket(), from).Do()
		if err != nil {
			return err
		}
		entries, err := driver.gcs.Objects.List(driver.getUserBucket()).Prefix(from).Do()
		if err != nil {
			return err
		}

		for _, entry := range entries.Items {
			newName := strings.Replace(entry.Name, from, to, 1)
			_, err = driver.gcs.Objects.Copy(driver.getUserBucket(), entry.Name, driver.getUserBucket(), newName, nil).Do()
			if err != nil {
				return err
			}
			err = driver.gcs.Objects.Delete(driver.getUserBucket(), entry.Name).Do()
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = driver.gcs.Objects.Copy(driver.getUserBucket(), from, driver.getUserBucket(), to, nil).Do()
	err = driver.gcs.Objects.Delete(driver.getUserBucket(), from).Do()
	return err
}

// Implementation of goftp's MakeDir method for Google Storage
func (driver *GoogleStorageDriver) MakeDir(path string) error {
	dir := strings.TrimLeft(path, "/") + "/"
	fmt.Println("mkdir", dir)
	var s string
	reader := strings.NewReader(s)
	object := &storage.Object{
		Name: dir,
	}
	_, err := driver.gcs.Objects.Insert(driver.getUserBucket(), object).Media(reader).Do()
	return err
}

// Implementation of goftp's GetFile method for Google Storage
func (driver *GoogleStorageDriver) GetFile(objectName string, offset int64) (int64, io.ReadCloser, error) {
	objectName = strings.TrimLeft(objectName, "/")
	res, err := driver.gcs.Objects.Get(driver.getUserBucket(), objectName).Do()
	if err == nil {
		log.Infof("The media download link for %v/%v is %v.\n\n", driver.getUserBucket(), res.Name, res.MediaLink)
	} else {
		log.Fatal(driver.gcs, "Failed to get %s/%s: %s.", driver.getUserBucket(), objectName, err)
	}
	response, err := driver.httpClient.Get(res.MediaLink)
	log.Info(int64(res.Size))
	return int64(res.Size), response.Body, nil
}

// Implementation of goftp's PutFile method for Google Storage
func (driver *GoogleStorageDriver) PutFile(objectName string, data io.Reader, appendData bool) (int64, error) {
	objectName = strings.TrimLeft(objectName, "/")
	object := &storage.Object{
		Name: objectName,
	}
	if res, err := driver.gcs.Objects.Insert(driver.getUserBucket(), object).Media(data).Do(); err == nil {
		log.Info("Created object %v at location %v\n\n", res.Name, res.SelfLink)
	} else {
		log.Fatal(driver.gcs, "Objects.Insert failed: %v", err)
	}
	return 0, nil
}

// Factory for the driver
// The contents of the service account file should look like this:
// {
//   "type": "service_account",
//   "project_id": "REDACTED",
//   "private_key_id": "REDACTED",
//   "private_key": "REDACTED",
//   "client_email": "REDACTED",
//   "client_id": "REDACTED",
//   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
//   "token_uri": "https://accounts.google.com/o/oauth2/token",
//   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
//   "client_x509_cert_url": "REDACTED"
// }

type GoogleStorageDriverFactory struct {
	Bucket             string
	ServiceAccountPath string
	BucketPerUser      bool
}

func NewGoogleStorageDriverFactory(bucket, serviceAccountPath string, bucketPerUser bool) server.DriverFactory {
	return &GoogleStorageDriverFactory{
		Bucket:             bucket,
		ServiceAccountPath: serviceAccountPath,
		BucketPerUser:      bucketPerUser,
	}
}

func (factory *GoogleStorageDriverFactory) NewDriver() (server.Driver, error) {
	token := getJWT(factory.ServiceAccountPath)
	httpClient := getClient(token)
	gcs := getService(httpClient)

	driver := &GoogleStorageDriver{
		curDir:        "/",
		bucket:        factory.Bucket,
		httpClient:    httpClient,
		gcs:           gcs,
		bucketPerUser: factory.BucketPerUser,
	}

	return driver, nil
}
