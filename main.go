package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/antchfx/htmlquery"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/op/go-logging"
	"golang.org/x/net/html"
)

const (
	tabletkiATCURL = "https://tabletki.ua/atc/"
	logLevel       = "INFO"
	workersNum     = 20
	csvFileName    = "tabletki.csv"
)

var log *logging.Logger

func initLogger(level string) {
	module := "drugs"
	log = logging.MustGetLogger(module)
	logLev, err := logging.LogLevel(level)
	if err != nil {
		logLev = logging.INFO
	}
	logging.SetLevel(logLev, module)
}

// DBConf is database credentials storage
type DBConf struct {
	Server   string
	Port     int
	User     string
	Password string
	Database string
}

// ConnString return GO SQL connection string to Open
func (dbc *DBConf) ConnString() string {
	return fmt.Sprintf(
		"server=%s;port=%d;user id=%s;password=%s;database=%s",
		dbc.Server, dbc.Port, dbc.User, dbc.Password, dbc.Database)
}

// DrugInfo contains drug name and link to the drug page
type DrugInfo struct {
	Name string
	Link string
}

// Drug struct contains all nececarry information about the drug
type Drug struct {
	Name         string
	Link         string
	Dosage       string
	Manufacture  string
	INN          string
	PharmGroup   string
	Registration string
	ATCCode      string
	Instruction  string
}

var drugFields = []string{
	"Name", "Link", "Dosage", "Manufacture", "INN",
	"PharmGroup", "Registration", "ATCCode", "Instruction"}

// Fields return list of the Drug field names
func (drug *Drug) Fields() []string {
	return drugFields
}

// Values return list of the Drug field values
func (drug *Drug) Values() []string {
	return []string{
		drug.Name, drug.Link, drug.Dosage, drug.Manufacture, drug.INN,
		drug.PharmGroup, drug.Registration, drug.ATCCode, drug.Instruction}
}

func htmlText(baseNode *html.Node, xpath string) string {
	node := htmlquery.FindOne(baseNode, xpath)
	if node == nil {
		return ""
	}
	return strings.TrimSpace(htmlquery.InnerText(node))
}

func fetchATCLinks(url string) ([]string, error) {
	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return []string{}, fmt.Errorf("HTTP request %s error: %s", url, err)
	}

	atcLinkNodes := htmlquery.Find(doc, `//div[contains(@id, "ATCPanel")]/ul/li/a`)
	atcLinks := make([]string, len(atcLinkNodes))
	for i, linkNode := range atcLinkNodes {
		atcLinks[i] = "https:" + htmlquery.SelectAttr(linkNode, "href")
	}

	return atcLinks, nil
}

func fetchDrugLinks(url string) ([]*DrugInfo, error) {
	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return []*DrugInfo{}, fmt.Errorf("HTTP request %s error: %s", url, err)
	}

	drugNodes := htmlquery.Find(doc, `//div[contains(@id, "GoodsListPanel")]/div/a`)

	drugsInfo := make([]*DrugInfo, len(drugNodes))
	for i, drugNode := range drugNodes {
		drugsInfo[i] = &DrugInfo{
			Name: htmlquery.SelectAttr(drugNode, "title"),
			Link: "https:" + htmlquery.SelectAttr(drugNode, "href")}
	}

	return drugsInfo, nil
}

func fetchDrug(drugInfo *DrugInfo) (*Drug, error) {
	doc, err := htmlquery.LoadURL(drugInfo.Link)
	if err != nil {
		return nil, fmt.Errorf("HTTP request %s error: %s", drugInfo.Link, err)
	}

	infoTable := htmlquery.FindOne(doc, `//div[contains(@id, "InstructionPanel")]/table/tbody`)
	if infoTable == nil {
		return &Drug{
			Name: drugInfo.Name,
			Link: drugInfo.Link}, nil
	}

	instruction := htmlText(doc, `//div[@itemprop="description"]`)
	instruction = strings.Replace(instruction, "Перевести на русский язык:", "", 1)
	instruction = strings.Replace(instruction, "Перевести", "", 1)
	instruction = strings.TrimSpace(instruction)

	dosage := htmlText(infoTable, `./tr/td[contains(text(), "Дозировка")]/following-sibling::td`)
	manufacture := htmlText(infoTable, `./tr/td[contains(text(), "Производитель")]/following-sibling::td`)
	inn := htmlText(infoTable, `./tr/td[contains(text(), "МНН")]/following-sibling::td`)
	pharmGroup := htmlText(infoTable, `./tr/td[contains(text(), "группа")]/following-sibling::td`)
	registration := htmlText(infoTable, `./tr/td[contains(text(), "Регистрация")]/following-sibling::td`)

	atcCodeNodes := htmlquery.Find(infoTable, `./tr/td[contains(text(), "Код АТХ")]/following-sibling::td/div`)
	codes := make([]string, len(atcCodeNodes))
	for i, atcNode := range atcCodeNodes {
		codes[i] = htmlText(atcNode, `./b`) + " - " + htmlText(atcNode, `./a/span`)
	}
	atcCode := strings.Join(codes, "\n")

	return &Drug{
		Name:         drugInfo.Name,
		Link:         drugInfo.Link,
		Dosage:       dosage,
		Manufacture:  manufacture,
		INN:          inn,
		PharmGroup:   pharmGroup,
		Registration: registration,
		ATCCode:      atcCode,
		Instruction:  instruction}, nil
}

func checkFatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func checkError(err error) bool {
	if err != nil {
		log.Error(err)
		return true
	}
	return false
}

func saveToCSV(drugsChan <-chan *Drug, fileName string) {
	file, err := os.OpenFile(
		csvFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	checkFatalError(err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// skip Instruction because it too long
	rowLen := len(drugFields) - 1

	// write CSV headers
	err = writer.Write(drugFields[:rowLen])
	checkFatalError(err)

	for drug := range drugsChan {
		err = writer.Write(drug.Values()[:rowLen])
		checkFatalError(err)
	}
}

func saveToMSSQL(drugsChan <-chan *Drug, dbc DBConf) int {
	db, err := sql.Open("sqlserver", dbc.ConnString())
	checkFatalError(err)
	defer db.Close()

	err = db.Ping()
	checkFatalError(err)

	_, err = db.Exec("USE drugs")
	checkFatalError(err)

	_, err = db.Exec("TRUNCATE TABLE Drugs")
	checkFatalError(err)

	totalCount := 0
	insertQuery := "INSERT INTO Drugs VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9)"

	batchCount := 0
	tx, err := db.Begin()

	for drug := range drugsChan {
		_, err = tx.Exec(insertQuery,
			drug.Name, drug.Link, drug.Dosage, drug.Manufacture, drug.INN,
			drug.PharmGroup, drug.Registration, drug.ATCCode, drug.Instruction)
		if err != nil {
			tx.Rollback()
			log.Fatal(err)
		}

		batchCount++
		if batchCount%100 == 0 {
			err = tx.Commit()
			checkFatalError(err)
			totalCount += batchCount
			batchCount = 0
			tx, err = db.Begin()
		}
	}

	if batchCount > 0 {
		err = tx.Commit()
		checkFatalError(err)
		totalCount += batchCount
	}

	return totalCount
}

func scan() {
	// Extract root ATC links
	log.Infof("Extract root ATC links from %s", tabletkiATCURL)

	links, err := fetchATCLinks(tabletkiATCURL)
	checkFatalError(err)
	log.Infof("Collected %d ATC links", len(links))

	// Extract drug names and links
	drugInChanel := make(chan *DrugInfo)

	go func() {
		defer close(drugInChanel)

		for _, link := range links {
			log.Infof("Loading ATC page %s", link)
			drugInfos, err := fetchDrugLinks(link)
			if checkError(err) {
				continue
			}

			log.Infof("Collected %d drugs names from %s", len(drugInfos), link)
			for _, drugInfo := range drugInfos {
				drugInChanel <- drugInfo
			}
		}
	}()

	// Extract drug information
	drugOutChanel := make(chan *Drug)

	go func() {
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, workersNum)
		defer close(semaphore)

		defer close(drugOutChanel)

		log.Info("Loading drug pages")

		counter := 0
		for drugInfo := range drugInChanel {
			wg.Add(1)
			semaphore <- struct{}{}
			counter++

			go func(di *DrugInfo, num int) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				log.Debugf("=> %s (%s)", di.Name, di.Link)
				drug, err := fetchDrug(di)
				if err != nil {
					log.Errorf(
						"Error loading drug \"%s\" (%s): %s",
						di.Name, di.Link, err)
					return
				}

				if num%100 == 0 {
					log.Infof("Scanned %d drugs", num)
				}

				drugOutChanel <- drug
			}(drugInfo, counter)
		}

		wg.Wait()
		log.Infof("Scanned %d drugs", counter)
	}()

	// Save drugs in CSV file
	// log.Infof("Save drugs to CSV %s", csvFileName)
	// saveToCSV(drugOutChanel, csvFileName)

	dbc := DBConf{
		Server: "localhost", Port: 1433,
		User: "root", Password: "123",
		Database: "drugs"}
	log.Infof("Save drugs to MSSQL %s:%d", dbc.Server, dbc.Port)
	totalRowsSaved := saveToMSSQL(drugOutChanel, dbc)
	log.Infof("Saved %d drugs to MSSQL %s:%d", totalRowsSaved, dbc.Server, dbc.Port)
}

func main() {
	start := time.Now()

	initLogger(logLevel)
	log.Info("Starting drugs scan")

	scan()
	log.Infof("Done in %s", time.Since(start))
}
