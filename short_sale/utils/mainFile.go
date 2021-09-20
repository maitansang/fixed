package utils

import (
	"archive/zip"
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"

	// "net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)
type  Short_Sale_Transactions1 struct {
	ID string `gorm:"primaryKey;autoIncrement:false"`
	MarketCenter string `json:"marketcenter" `
	Symbol string `json:"symbol" `
	Date string `json:"dt" `
	Time string `json:"tm" `
	ShortType string `json:"shorttype" `
	Size string `json:"size" `
	Price string `json:"price" `
	FileName string `json: "filename"`
	// LinkIndicator string `json:"" `
}
// func (user *Short_Sale_Transactions1) BeforeCreate(scope *gorm.Scope) error {
//     scope.SetColumn("ID", uuid.NewV4())
//     return nil
// }
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
func ClearFile(specUrl string) error{
	absPath1, _ := filepath.Abs("../short_sale/extract/"+ specUrl + ".txt")
	absPath2, _ := filepath.Abs("../short_sale/"+ specUrl + ".zip")

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
func MainFunc() {
	
	fmt.Println("Ok")
	if len(os.Args)==1{
		log.Println("please enter specUrl")
		return
	}
	specUrl := os.Args[1]
	log.Println("----",specUrl)

	err :=ClearFile(specUrl)
	if err != nil {
		log.Println(err)
	}
	

	resp, err := http.Get("https://cdn.finra.org/equity/regsho/monthly/"+specUrl+".zip")
	if err != nil {
		fmt.Printf("err: %s", err)
	}

	defer resp.Body.Close()
	fmt.Println("status", resp.Status)
	if resp.StatusCode != 200 {
		return
	}

	// Create the file
	out, err := os.Create(specUrl+".zip")
	if err != nil {
		fmt.Printf("err: %s", err)
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)

	err = Unzip(specUrl+".zip", "extract/")
	if err != nil {
		log.Println("err when extract ", err)
	}

	// handle db
	dsn := "host=52.116.150.66 user=dev_user password=Dev$54321 dbname=transaction_db port=5433 sslmode=disable"
	 db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	 if err != nil {
		log.Println("can not open db")
	}

	data := Short_Sale_Transactions1{}
	db.Take(&data)

	absPath, _ := filepath.Abs("../short_sale/extract/"+specUrl + ".txt")
	text,err:=ReadFileLineByLine(absPath)
	if err != nil {
		log.Println("can not read file")
	}

	arrTrans := ParseData(text, specUrl)
	db.AutoMigrate(&Short_Sale_Transactions1{})
	db.Session(&gorm.Session{AllowGlobalUpdate: true}).Where("filename = ?", specUrl) .Delete(&Short_Sale_Transactions1{})
	db.Create(&arrTrans)
	err =ClearFile(specUrl)
	if err != nil {
		log.Println(err)
	}
}

func ParseData(text []string, specUrl string)[]Short_Sale_Transactions1{
	var arrTrans []Short_Sale_Transactions1 
	for _, t := range text[1:] {
		fields := strings.Split(t, "|")
		trans := Short_Sale_Transactions1{
			ID: uuid.NewString(),
			MarketCenter: fields[0],
			Symbol: fields[1],
			Date: fields[2],
			Time: fields[3],
			ShortType: fields[4],
			Size: fields[5],
			Price: fields[6],
			FileName: specUrl,
		}
		arrTrans = append(arrTrans, trans)
	}
	return arrTrans
}
func ReadFileLineByLine(nameFile string) ([]string, error){
	// os.Open() opens specific file in 
    // read-only mode and this return 
    // a pointer of type os.
    file, err := os.Open(nameFile)
  
    if err != nil {
        log.Fatalf("failed to open", err)
  
    }
  
    // The bufio.NewScanner() function is called in which the
    // object os.File passed as its parameter and this returns a
    // object bufio.Scanner which is further used on the
    // bufio.Scanner.Split() method.
    scanner := bufio.NewScanner(file)
  
    // The bufio.ScanLines is used as an 
    // input to the method bufio.Scanner.Split()
    // and then the scanning forwards to each
    // new line using the bufio.Scanner.Scan()
    // method.
    scanner.Split(bufio.ScanLines)
    var text []string
  
    for scanner.Scan() {
        text = append(text, scanner.Text())
    }
  
    // The method os.File.Close() is called
    // on the os.File object to close the file
    file.Close()
  
    // and then a loop iterates through 
    // and prints each of the slice values.
    for _, each_ln := range text {
        fmt.Println(each_ln)
    }
	return text, err
}