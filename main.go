package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/antchfx/htmlquery"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/integrii/flaggy"
	"github.com/op/go-logging"
	"golang.org/x/net/html"
)

// ----- Config -----

const (
	version        = "1.1.0"
	tabletkiATCURL = "https://tabletki.ua/atc/"
	logLevel       = "INFO"
)

// Config is project settings storage
type Config struct {
	Prod         bool
	WorkersNum   int
	CSVFileName  string
	JSONFileName string
	MSSQLConnURL string
}

func getConfig() Config {
	return Config{
		Prod:         false,
		WorkersNum:   20,
		CSVFileName:  "tabletki.csv",
		JSONFileName: "ATC_tree.json",
		MSSQLConnURL: "sqlserver://user:pass@localhost:1433?database=drugs"}
}

// ----- Logger -----

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

// ----- Helpers -----

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

func htmlText(baseNode *html.Node, xpath string) string {
	node := htmlquery.FindOne(baseNode, xpath)
	if node == nil {
		return ""
	}
	return strings.TrimSpace(htmlquery.InnerText(node))
}

// ----- ATC Tree -----

// ATCTree is the tree of ATC classification from the site
type ATCTree struct {
	Name     string     `json:"name"`
	Link     string     `json:"-"`
	Children []*ATCTree `json:"children"`
}

func fetchATCTree(tree *ATCTree) error {
	log.Debugf("|-- %s", tree.Link)
	doc, err := htmlquery.LoadURL(tree.Link)
	if err != nil {
		return fmt.Errorf("HTTP request %s error: %s", tree.Link, err)
	}

	childrenNodes := htmlquery.Find(doc, `//div[contains(@id, "ATCPanel")]/ul/li/a`)
	numOfChildren := len(childrenNodes)

	tree.Children = make([]*ATCTree, numOfChildren)
	if numOfChildren == 0 {
		fmt.Print("-")
		return nil
	}

	for i, childNode := range childrenNodes {
		tree.Children[i] = &ATCTree{
			Name: htmlquery.SelectAttr(childNode, "title"),
			Link: "https:" + htmlquery.SelectAttr(childNode, "href"),
		}
	}

	var wg sync.WaitGroup
	wg.Add(numOfChildren)
	res := make(chan error, numOfChildren)

	for _, child := range tree.Children {
		go func(c *ATCTree) {
			defer wg.Done()
			res <- fetchATCTree(c)
		}(child)
	}

	wg.Wait()
	close(res)

	for err := range res {
		if err != nil {
			return err
		}
	}

	fmt.Print("\n|")
	return nil
}

func scanATCTree(cnf Config) {
	tree := &ATCTree{
		Name:     "АТХ (ATC) классификация",
		Link:     tabletkiATCURL,
		Children: make([]*ATCTree, 0)}

	// Load ATCTree
	log.Info("Load ATC tree recursively")
	err := fetchATCTree(tree)
	checkFatalError(err)

	// Convert ATCTree names to json tree
	log.Info("Convert ATC tree to JSON")
	treeJSON, err := json.MarshalIndent(tree, "", "  ")
	checkFatalError(err)

	// Save results
	if cnf.Prod {
		// Save ATC tree MSSQL database
		log.Info("Save ATC tree to MSSQL")
		db, err := sql.Open("sqlserver", cnf.MSSQLConnURL)
		checkFatalError(err)
		defer db.Close()

		err = db.Ping()
		checkFatalError(err)
		_, err = db.Exec("TRUNCATE TABLE ATCTree")
		checkFatalError(err)

		_, err = db.Exec("INSERT INTO ATCTree VALUES (@p1)", string(treeJSON))
		checkFatalError(err)

	} else {
		// Save ATC tree to JSON file
		log.Infof("Save ATC tree to JSON %s", cnf.JSONFileName)
		file, err := os.OpenFile(
			cnf.JSONFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
		checkFatalError(err)
		defer file.Close()

		file.Write(treeJSON)
	}
}

// ----- Drugs -----

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

func fetchDrugATCLinks(url string) ([]string, error) {
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

func fetchDrugBaseLinks(url string) ([]string, error) {
	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return []string{}, fmt.Errorf("HTTP request %s error: %s", url, err)
	}

	drugBaseLinkNodes := htmlquery.Find(doc, `//div[contains(@id, "GoodsListPanel")]/div/a`)

	drugBaseLinks := make([]string, len(drugBaseLinkNodes))
	for i, linkNode := range drugBaseLinkNodes {
		drugBaseLinks[i] = "https:" + htmlquery.SelectAttr(linkNode, "href")
	}

	return drugBaseLinks, nil
}

func fetchDrugLinks(url string) ([]string, error) {
	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return []string{}, fmt.Errorf("HTTP request %s error: %s", url, err)
	}

	drugLinkNodes := htmlquery.Find(doc, `//div[@class="search-control-panel"]/div/div/ul/li/a`)
	if len(drugLinkNodes) < 2 {
		log.Warningf("Drug links for %s not found", url)
		return []string{}, nil
	}

	// Skip first link "Все дозировки"
	if htmlquery.InnerText(drugLinkNodes[0]) != "Все дозировки" {
		log.Warningf(
			"Unexpected first link %s for %s", 
			htmlquery.SelectAttr(drugLinkNodes[0], "href"), url)
	}
	drugLinkNodes = drugLinkNodes[1:]

	drugLinks := make([]string, len(drugLinkNodes))
	for i, linkNode := range drugLinkNodes {
		drugLinks[i] = "https:" + htmlquery.SelectAttr(linkNode, "href")
	}

	return drugLinks, nil
}

func fetchDrug(url string) (Drug, error) {
	log.Debugf("=> %s", url)
	doc, err := htmlquery.LoadURL(url)
	if err != nil {
		return Drug{}, fmt.Errorf("HTTP request %s error: %s", url, err)
	}

	name := htmlText(doc, `//div[@class="header-panel"]/h1`)

	instruction := htmlText(doc, `//div[@itemprop="description"]`)
	instruction = strings.Replace(instruction, "Перевести на русский язык:", "", 1)
	instruction = strings.Replace(instruction, "Перевести", "", 1)
	instruction = strings.TrimSpace(instruction)

	infoTable := htmlquery.FindOne(doc, `//div[contains(@id, "InstructionPanel")]/table/tbody`)
	if infoTable == nil {
		return Drug{
			Name:        name,
			Link:        url,
			Instruction: instruction}, nil
	}

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

	return Drug{
		Name:         name,
		Link:         url,
		Dosage:       dosage,
		Manufacture:  manufacture,
		INN:          inn,
		PharmGroup:   pharmGroup,
		Registration: registration,
		ATCCode:      atcCode,
		Instruction:  instruction}, nil
}

func saveDrugsToCSV(drugsChan <-chan Drug, fileName string) {
	file, err := os.OpenFile(
		fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	checkFatalError(err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Skip Instruction because it too long

	// Write CSV headers
	headers := []string{
		"Name", "Link", "Dosage", "Manufacture",
		"INN", "PharmGroup", "Registration", "ATCCode"}
	err = writer.Write(headers)
	checkFatalError(err)

	num := 0
	for drug := range drugsChan {
		row := []string{
			drug.Name, drug.Link, drug.Dosage, drug.Manufacture,
			drug.INN, drug.PharmGroup, drug.Registration, drug.ATCCode}
		err = writer.Write(row)
		checkFatalError(err)

		num++
		if num%100 == 0 {
			log.Infof("Scanned %d drugs", num)
		}
	}

	log.Infof("Scanned %d drugs", num)
}

func saveDrugsToMSSQL(drugsChan <-chan Drug, mssqlConnURL string) int {
	db, err := sql.Open("sqlserver", mssqlConnURL)
	checkFatalError(err)
	defer db.Close()

	err = db.Ping()
	checkFatalError(err)
	_, err = db.Exec("TRUNCATE TABLE Drugs")
	checkFatalError(err)

	totalCount := 0
	insertQuery := "INSERT INTO Drugs VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9)"

	batchCount := 0
	tx, err := db.Begin()
	checkFatalError(err)

	num := 0
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

		num++
		if num%100 == 0 {
			log.Infof("Scanned %d drugs", num)
		}
	}

	if batchCount > 0 {
		err = tx.Commit()
		checkFatalError(err)
		totalCount += batchCount
	}

	log.Infof("Scanned %d drugs", num)
	return totalCount
}

func linksMultiFetcher(
	inChan chan string, workersNum int, 
	fetcher func(string) ([]string, error)) chan string {
	
	var wg sync.WaitGroup
	outChan := make(chan string)

	for w := 0; w < workersNum; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for link := range inChan {
				subLinks, err := fetcher(link)
				if checkError(err) {
					continue
				}
				for _, subLink := range subLinks {
					outChan <- subLink
				}
			}					
		}()
	}

	go func() {
		wg.Wait()
		close(outChan)
	}()

	return outChan
}

func scanDrugs(cnf Config) {
	log.Infof("Start drugs scrapping from %s", tabletkiATCURL)

	rootCh := make(chan string, 1)
	rootCh <- tabletkiATCURL
	close(rootCh)

	// Extract drug links
	atcLinksCh := linksMultiFetcher(rootCh, 1, fetchDrugATCLinks)
	baseLinksCh := linksMultiFetcher(atcLinksCh, 1, fetchDrugBaseLinks)
	drugLinksCh := linksMultiFetcher(baseLinksCh, cnf.WorkersNum, fetchDrugLinks)

	// Fetch drug info 
	var wg sync.WaitGroup
	drugsCh := make(chan Drug)

	for w := 0; w < cnf.WorkersNum; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for link := range drugLinksCh {
				drug, err := fetchDrug(link)
				if checkError(err) {
					continue
				}
				drugsCh <- drug
			}					
		}()
	}

	go func() {
		wg.Wait()
		close(drugsCh)
	}()

	// Save scan results
	if cnf.Prod {
		// Save drugs to MSSQL database
		log.Info("Save drugs to MSSQL")
		totalRowsSaved := saveDrugsToMSSQL(drugsCh, cnf.MSSQLConnURL)
		log.Infof("Saved %d drugs to MSSQL", totalRowsSaved)
	} else {
		// Save drugs to CSV file
		log.Infof("Save drugs to CSV %s", cnf.CSVFileName)
		saveDrugsToCSV(drugsCh, cnf.CSVFileName)
	}
}

// ----- Main -----

func main() {
	start := time.Now()
	cnf := getConfig()
	initLogger(logLevel)

	flaggy.SetName("tabletki")
	flaggy.SetDescription(fmt.Sprintf(
		"This programm extract and save information "+
			"about the drugs and ATC classification "+
			"from the \"%s\" link.", tabletkiATCURL))
	flaggy.SetVersion(version)

	flaggy.Bool(&cnf.Prod, "", "prod", "Set PRODUCTION mode (save results to MSSQL DB)")
	flaggy.Int(&cnf.WorkersNum, "", "workers", "Number of workers to run scan in parralel")
	flaggy.String(&cnf.CSVFileName, "", "csvfile", "Name of CSV file where save drugs in debug mode")
	flaggy.String(&cnf.JSONFileName, "", "jsonfile", "Name of JSON file where save ATC tree in debug mode")
	flaggy.String(&cnf.MSSQLConnURL, "", "mssqlurl", "MSSQL database connection url")

	atctreeSubCmd := flaggy.NewSubcommand("atctree")
	flaggy.AttachSubcommand(atctreeSubCmd, 1)
	drugsSubCmd := flaggy.NewSubcommand("drugs")
	flaggy.AttachSubcommand(drugsSubCmd, 1)

	flaggy.Parse()

	if atctreeSubCmd.Used {
		log.Infof("Starting ATC classification scan (production: %t)", cnf.Prod)
		scanATCTree(cnf)
	} else if drugsSubCmd.Used {
		log.Infof("Starting drugs scan (production: %t, workers: %d)", cnf.Prod, cnf.WorkersNum)
		scanDrugs(cnf)
	} else {
		log.Info("No subcommand selected!")
	}

	log.Infof("Done in %s", time.Since(start))
}
