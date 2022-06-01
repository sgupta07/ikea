package main

import (
	"LogWatcher/file"
	"LogWatcher/shared"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Rican7/retry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/natefinch/lumberjack.v2"
)

//Body ... holds file event data and allows that data
// to be passed over channels by pointers
type Body struct {
	FilePath string
	ModTime  time.Time
	ModType  string
	Size     int64
}

type Config struct {
	Filename   string
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
	Directory  string
	OutputDir  string
	//SRC                 string
	ENDPOINT            string
	REGION              string
	ACCESSKEY           string
	SECRETACCESSKEY     string
	ENVBUCKTEPREFIX     string
	ID                  string
	STATUS              string
	NONCURRENTDAYS      int64
	EXPIRATION          int64
	DAYSAFTERINITIATION int64
}

var (
	watcher       *fsnotify.Watcher
	LogFileName   string
	LogMaxSize    int
	LogMaxBackups int
	LogMaxAge     int
	LogCompress   bool
	Directory     string
	OutputDir     string
	//SRC                 string
	ENDPOINT            string
	REGION              string
	ACCESSKEY           string
	SECRETACCESSKEY     string
	ENVBUCKTEPREFIX     string
	ID                  string
	STATUS              string
	NONCURRENTDAYS      int64
	EXPIRATION          int64
	DAYSAFTERINITIATION int64
	hostname            string
	err                 error
	config              Config
)

func main() {

	config := ReadConfig()
	Directory = config.Directory
	OutputDir = config.OutputDir
	LogFileName = config.Filename
	LogMaxSize = config.MaxSize
	LogMaxBackups = config.MaxBackups
	LogMaxAge = config.MaxAge
	LogCompress = config.Compress

	//S3 and OLD config variable
	//SRC = config.SRC
	ENDPOINT = config.ENDPOINT
	REGION = config.REGION
	ACCESSKEY = config.ACCESSKEY
	SECRETACCESSKEY = config.SECRETACCESSKEY
	ENVBUCKTEPREFIX = config.ENVBUCKTEPREFIX
	ID = config.ID
	STATUS = config.STATUS
	NONCURRENTDAYS = config.NONCURRENTDAYS
	EXPIRATION = config.EXPIRATION
	DAYSAFTERINITIATION = config.DAYSAFTERINITIATION

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(&lumberjack.Logger{
		Filename:   LogFileName,
		MaxSize:    LogMaxSize, // megabytes
		MaxBackups: LogMaxBackups,
		MaxAge:     LogMaxAge,   //days
		Compress:   LogCompress, // disabled by default
	})

	//create s3 session and create bucket
	var name, hosterr = os.Hostname()

	if hosterr != nil {
		log.Fatalln(err)
	}

	hostname = config.ENVBUCKTEPREFIX + strings.ToLower(name)

	//Creating custome resolver for cloudian s3 endpoint
	myCustomResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.S3ServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           config.ENDPOINT,
				SigningRegion: config.REGION,
			}, nil
		}

		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}
	//Creating Aws session to use later with acess key and secret keys
	mySession := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(config.REGION),
		Credentials:      credentials.NewStaticCredentials(config.ACCESSKEY, config.SECRETACCESSKEY, ""),
		EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
		LogLevel:         aws.LogLevel(aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors),
	}))

	createBucket(mySession, hostname)
	Filewatcher(mySession)

}

// Start file watcher process

//func Filewatcher(cha chan *Body) {
func Filewatcher(session *session.Session) {

	// creates a new file watcher
	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	// Walk each directory searching for nested directories

	if err := filepath.Walk(Directory, watchDir); err != nil {
		shared.Check(err)
	}
	log.Printf(" [*] Awaiting file modification event")
	//
	done := make(chan bool)

	// Loop until error or close, blocking on watcher.events and passing back data over channel cha
	go func() {
		for {
			select {
			// watch for events
			case event := <-watcher.Events:
				File := event.Name
				Extension := path.Ext(event.Name)

				log.Printf(" Event String:  %s", event.Op.String())

				if event.Op.String() == "WRITE" && Extension == ".log" {
					if !strings.HasSuffix(File, ".snapshot.log") && !strings.Contains(File, "anana") {
						log.Printf(" inside 2nd condition File Variable :  %s", File)
						log.Printf(" inside 2nd condition Extension:  %s", Extension)

						//var eventBody *Body
						eventBody := Body{}
						//var eventBody *Body = <-cha //receive
						t := time.Now()
						eventBody.ModTime = t.Round(time.Second)
						eventBody.ModType = event.Op.String()
						eventBody.FilePath = event.Name
						fi, err := os.Stat(eventBody.FilePath)
						if err != nil {
							shared.Check(err)
						}
						eventBody.Size = fi.Size()
						log.Printf(" Size :  %d", eventBody.Size)

						FileName := filepath.Base(eventBody.FilePath)
						FilePath := eventBody.FilePath
						outputzip := OutputDir + "\\" + FileName + ".zip"
						log.Printf(" [x] Log output directory:  %s", outputzip)
						log.Printf(" Base Filename:  %s", FileName)
						log.Printf(" FilePath:  %s", FilePath)
						log.Printf(" without remove1:  %s", OutputDir)
						//PrintMemUsage()
						go removeFile(OutputDir)

						go func() {
							cha1 := make(chan string)
							go file.ZipFiles(cha1, outputzip, FilePath)
							// 	// 	// 	//go testlog()
							// 	// 	// 	//var zipfilepath string

							zipfilepath := <-cha1
							go uploadFile(session, zipfilepath, FilePath, hostname)
							log.Printf(" Zip file path :  %s", zipfilepath)
						}()
						//cha <- eventBody
					}
				}

				// watch for errors
			case err := <-watcher.Errors:
				shared.Check(err)
			}

		}

	}()

	<-done
}

func watchDir(path string, fi os.FileInfo, err error) error {

	// Add Watchers to each nested directory
	if fi.Mode().IsDir() {
		return watcher.Add(path)
	}

	return nil
}

// Reads info from config file
func ReadConfig() Config {
	var configfile = "config.properties"
	_, err := os.Stat(configfile)
	if err != nil {
		shared.Check(err)
	}

	//var config1 Config
	if _, err := toml.DecodeFile(configfile, &config); err != nil {
		shared.Check(err)
	}

	return config
}

//s3 connection and upload function
func createBucket(session *session.Session, bucket string) {

	///var lifecycleconf = LoadConfiguration("../../lifcyle.json") //Loading config file
	// Open our jsonFile

	svc := s3.New(session)

	_, err = svc.CreateBucket(&s3.CreateBucketInput{
		Bucket:                     aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})

	if err != nil {
		shared.Check(err)
	}

	// Wait until bucket is created before finishing
	log.Printf("Waiting for bucket %q to be created...\n", bucket)

	err = svc.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		shared.Check(err)
	}

	log.Printf("Bucket %q successfully created\n", bucket)

	// PUT BUCKET LIFECYCLE CONFIGURATION
	// Replace <BUCKET_NAME> with the name of the bucket
	lInput := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Status: aws.String(config.STATUS),
					Filter: &s3.LifecycleRuleFilter{},
					ID:     aws.String(config.ID),
					NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
						NoncurrentDays: aws.Int64(config.NONCURRENTDAYS),
					},
					AbortIncompleteMultipartUpload: &s3.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int64(config.DAYSAFTERINITIATION),
					},
				},
			},
		},
	}

	l, e := svc.PutBucketLifecycleConfiguration(lInput)
	log.Println(l) // should print an empty bracket
	log.Println(e) // should print <nil>

	// GET BUCKET LIFECYCLE CONFIGURATION
	gInput := &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	}

	g, e := svc.GetBucketLifecycleConfiguration(gInput)
	log.Println(g)
	log.Println(e) // see response for results
	log.Println(config.EXPIRATION)
	//objectlock configuration
	//objectlock configuration
	lockInput := &s3.PutObjectLockConfigurationInput{
		Bucket:                  aws.String(bucket),
		ExpectedBucketOwner:     new(string),
		ObjectLockConfiguration: &s3.ObjectLockConfiguration{ObjectLockEnabled: aws.String(config.STATUS), Rule: &s3.ObjectLockRule{DefaultRetention: &s3.DefaultRetention{Days: aws.Int64(config.EXPIRATION), Mode: aws.String("Compliance"), Years: nil}}},
		RequestPayer:            new(string),
		Token:                   new(string),
	}

	lock, eerro := svc.PutObjectLockConfigurationRequest(lockInput)
	log.Println(lock) // should print an empty bracket
	log.Println(eerro)
}

func uploadFile(session *session.Session, uploadFileDir string, actualFilePath string, bucket string) error {

	log.Println("Before OpenFile : " + bucket)

	var upFile *os.File

	err := retry.Retry(func(attempt uint) error {
		var err error

		upFile, err = os.Open(uploadFileDir)

		return err
	})

	// upFile, err := os.OpenFile(uploadFileDir, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		shared.Check(err)
	}
	defer upFile.Close()

	log.Println("After OpenFile : " + bucket)

	upFileInfo, _ := upFile.Stat()
	var fileSize int64 = upFileInfo.Size()
	//fileBuffer := make([]byte, fileSize)
	filePath := filepath.Dir(actualFilePath)
	filePath = filepath.ToSlash(filePath)
	fileName := filepath.Base(upFile.Name())

	log.Println(fileSize)

	log.Println("Bucket and Hostname : " + bucket)
	log.Println("File Name: " + fileName)
	log.Println("File Path: " + filePath)
	h, m, s := time.Now().Local().Clock()
	date := time.Now()

	cons := fmt.Sprint("h:", h, "m:", m, "s", s)
	hourf := fmt.Sprint(h)
	datef := fmt.Sprint(date.Format("01-02-2006"))
	log.Println(cons)
	log.Println(hourf)
	log.Println(datef)

	uploader := s3manager.NewUploader(session, func(u *s3manager.Uploader) {
		u.PartSize = 100 * 1024 * 1024 // 100MB per part
	})

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(path.Join(filePath, datef, hourf, fileName)),
		ACL:                  aws.String("public-read"),
		Body:                 upFile,
		ServerSideEncryption: aws.String("AES256"),
		//ContentMD5:           aws.String("true"),
	})

	if err != nil {
		log.Println(err.Error())
		shared.Check(err)

	}

	log.Printf("file uploaded to, %s\n", result.Location)

	return err
}

func isOlderThanOnehour(t time.Time) bool {
	return time.Since(t) > 1*time.Hour
}

func removeFile(uploadFileDir string) (files []os.FileInfo, err error) {
	tmpfiles, err := ioutil.ReadDir(uploadFileDir)
	if err != nil {
		return
	}

	for _, file := range tmpfiles {
		if file.Mode().IsRegular() {
			//log.Print(path.Join(uploadFileDir, file.Name()))
			if isOlderThanOnehour(file.ModTime()) {
				log.Print(file.Name())
				e := os.Remove(path.Join(uploadFileDir, file.Name()))
				if e != nil {
					shared.Check(e)
				}
			}
		}
	}
	return
}

// func exitErrorf(msg string, args ...interface{}) {
// 	log.Fatal("exitErrorf:" + msg)
// }
