package utils

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type DB struct {
	*sqlx.DB
}

func InitDB() (*DB, error) {
	db, err := sqlx.Open("postgres", "host=52.116.150.66 user=postgres dbname=stockmarket password=P`AgD!9g!%~hz3M<	sslmode=disable")
	if err != nil {
		return nil, errors.Wrap(err, "connect to postgres:")
	}
	db.SetMaxOpenConns(150)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(60 * time.Minute)
	/*
			qry := `CREATE TABLE IF NOT EXISTS short_interest (
			id BIGSERIAL PRIMARY KEY,
			date date,
			ticker text,
			short bigint,
			shortexempt bigint,

			UNIQUE(date,sym)
			)

		qry := `CREATE TABLE IF NOT EXISTS tradesraw_top01 (
			id BIGSERIAL PRIMARY KEY,
			date date,
			ticker text,
			x integer,
			z integer,
			p real,
			s bigint,
			c integer[],
			t bigint,
			time time,
			UNIQUE(sym,t)
		)`

		_, err = db.Exec(qry)
		if err != nil {
			log.Fatalln("cannot create table tradesraw_top01", err)
		}
	*/
	// for i := 0; i < 12; i++ {
	// 	qry := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS tradesraw_temp%d (
	// 		id BIGSERIAL PRIMARY KEY,
	// 		ev text,
	// 		ticker text,
	// 		x integer,
	// 		i bigint,
	// 		z integer,
	// 		p real,
	// 		s bigint,
	// 		c integer[],
	// 		t bigint
	// 	)`, i)
	// 	_, err = db.Exec(qry)
	// 	if err != nil {
	// 		log.Fatalln("cannot create able", i, err)
	// 	}
	// }

	return &DB{
		db,
	}, nil
}

func MainFunc() {
	loc, err := time.LoadLocation("EST")
	if err != nil {
		log.Fatalln("Can't set timezone", err)
	}
	time.Local = loc // -> this is setting the global timezone
	log.Println("time=", time.Now())

	db, err := InitDB()
	if err != nil {
		log.Fatalln("cant open DB", err)
	}
	_ = db
	defer db.Close()
	// shares, err := db.getFloat("AAPL")
	// if err != nil {
	// 	log.Fatalln("EERRROR:", err)
	// }
	// log.Println("got", "AAPL", shares)
	start, err := time.Parse("2006-01-02", os.Args[2])
	if err != nil {
		log.Fatalln("Can't parse time", err, os.Args[2], "Time must be in the format 2006-01-02")
	}

	// end := start.AddDate(0, 0, -4)
	// end, _ := time.Parse("2006-01-02", "2019-01-01")
	// end, _ := time.Parse("2006-01-02", "2018-08-26")
	end, err := time.Parse("2006-01-02", os.Args[1])
	if err != nil {
		log.Fatalln("Can't parse time", err, os.Args[1], "Time must be in the format 2006-01-02")
	}

	//log.Fatalln(db.getShortIneterest(`20210129`))

	for t := start; t.After(end); t = t.AddDate(0, 0, -1) {
		if t.Weekday() == 0 || t.Weekday() == 6 {
			continue
		}
		err := db.getShortIneterest(t.Format("20060102"))
		if err != nil {
			log.Println("ERROR getshortinterest", err)
		}
	}
}

type jsonResult struct {
	Results []Result `json:"results"`
}

type Result struct {
	Shares int64 `json:"shares"`
}

const URL_FINANCIALS = `https://api.polygon.io/v2/reference/financials/%s?limit=1&type=Q&apiKey=6irkrzg7Nf9_s7qVpAscTAMeesF8eFu0`

func (db DB) getFloat(ticker string) (int64, error) {
	url := fmt.Sprintf(URL_FINANCIALS, ticker)
	res := jsonResult{}
	err := getJson(url, &res)
	if err != nil {
		log.Println("ERROR GETTING URL", url, err)
		return 0, err
	}
	return res.Results[0].Shares, nil
}

//const URL_SHORT = `http://regsho.finra.org/CNMSshvol%s.txt`
const URL_SHORT = `https://cdn.finra.org/equity/regsho/daily/CNMSshvol%s.txt`

func (db DB) getShortIneterest(date string) error {
	url := fmt.Sprintf(URL_SHORT, date)
	log.Println("getting ", url)
	resp, err := http.Get(url)
	if err != nil {
		return errors.Wrap(err, "GET SHORT URL")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "GET SHORT URL READ BODY")
	}
	//fmt.Println("BODY", string(body))
	// var lines []string
	// scanner := bufio.NewScanner(resp.Body)
	// scanner.Split(ScanLinesWithCR)
	// for scanner.Scan() {
	// 	lines = append(lines, scanner.Text())
	// }
	// if scanner.Err() != nil {
	// 	return errors.Wrap(err, "Can't scan body")
	// }
	//lines := strings.Split(strings.Replace(string(body), "\r\n", "\n", -1), "\n")
	newbody := string(body)
	newbody = strings.ReplaceAll(newbody, "\r", "")
	lines := strings.Split(newbody, "\n")
	fmt.Println(" len=", len(lines))
	for _, line := range lines[1:] {
		fields := strings.Split(line, "|")
		//fmt.Println(fields)
		if len(fields) < 4 {
			continue
		}
		//fmt.Println(fields[1], fields[2])
		_, err := db.Exec("INSERT INTO short_interest (date,ticker,short,shortexempt) VALUES($1,$2,$3,$4)",
			date, fields[1], fields[2], fields[3])
		if err != nil {
			log.Println("ERROR INSERTING", err)
		}
	}
	return nil
}

func ScanLinesWithCR(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\r'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

/*

{
 "status": "OK",
 "count": 1,
 "results": [
  {
   "ticker": "string",
   "period": "Q",
   "calendarDate": "2019-03-31",
   "reportPeriod": "2019-03-31",
   "updated": "1999-03-28",
   "accumulatedOtherComprehensiveIncome": 0,
   "assets": 0,
   "assetsAverage": 0,
   "assetsCurrent": 0,
   "assetTurnover": 0,
   "assetsNonCurrent": 0,
   "bookValuePerShare": 0,
   "capitalExpenditure": 0,
   "cashAndEquivalents": 0,
   "cashAndEquivalentsUSD": 0,
   "costOfRevenue": 0,
   "consolidatedIncome": 0,
   "currentRatio": 0,
   "debtToEquityRatio": 0,
   "debt": 0,
   "debtCurrent": 0,
   "debtNonCurrent": 0,
   "debtUSD": 0,
   "deferredRevenue": 0,
   "depreciationAmortizationAndAccretion": 0,
   "deposits": 0,
   "dividendYield": 0,
   "dividendsPerBasicCommonShare": 0,
   "earningBeforeInterestTaxes": 0,
   "earningsBeforeInterestTaxesDepreciationAmortization": 0,
   "EBITDAMargin": 0,
   "earningsBeforeInterestTaxesDepreciationAmortizationUSD": 0,
   "earningBeforeInterestTaxesUSD": 0,
   "earningsBeforeTax": 0,
   "earningsPerBasicShare": 0,
   "earningsPerDilutedShare": 0,
   "earningsPerBasicShareUSD": 0,
   "shareholdersEquity": 0,
   "averageEquity": 0,
   "shareholdersEquityUSD": 0,
   "enterpriseValue": 0,
   "enterpriseValueOverEBIT": 0,
   "enterpriseValueOverEBITDA": 0,
   "freeCashFlow": 0,
   "freeCashFlowPerShare": 0,
   "foreignCurrencyUSDExchangeRate": 0,
   "grossProfit": 0,
   "grossMargin": 0,
   "goodwillAndIntangibleAssets": 0,
   "interestExpense": 0,
   "investedCapital": 0,
   "investedCapitalAverage": 0,
   "inventory": 0,
   "investments": 0,
   "investmentsCurrent": 0,
   "investmentsNonCurrent": 0,
   "totalLiabilities": 0,
   "currentLiabilities": 0,
   "liabilitiesNonCurrent": 0,
   "marketCapitalization": 0,
   "netCashFlow": 0,
   "netCashFlowBusinessAcquisitionsDisposals": 0,
   "issuanceEquityShares": 0,
   "issuanceDebtSecurities": 0,
   "paymentDividendsOtherCashDistributions": 0,
   "netCashFlowFromFinancing": 0,
   "netCashFlowFromInvesting": 0,
   "netCashFlowInvestmentAcquisitionsDisposals": 0,
   "netCashFlowFromOperations": 0,
   "effectOfExchangeRateChangesOnCash": 0,
   "netIncome": 0,
   "netIncomeCommonStock": 0,
   "netIncomeCommonStockUSD": 0,
   "netLossIncomeFromDiscontinuedOperations": 0,
   "netIncomeToNonControllingInterests": 0,
   "profitMargin": 0,
   "operatingExpenses": 0,
   "operatingIncome": 0,
   "tradeAndNonTradePayables": 0,
   "payoutRatio": 0,
   "priceToBookValue": 0,
   "priceEarnings": 0,
   "priceToEarningsRatio": 0,
   "propertyPlantEquipmentNet": 0,
   "preferredDividendsIncomeStatementImpact": 0,
   "sharePriceAdjustedClose": 0,
   "priceSales": 0,
   "priceToSalesRatio": 0,
   "tradeAndNonTradeReceivables": 0,
   "accumulatedRetainedEarningsDeficit": 0,
   "revenues": 0,
   "revenuesUSD": 0,
   "researchAndDevelopmentExpense": 0,
   "returnOnAverageAssets": 0,
   "returnOnAverageEquity": 0,
   "returnOnInvestedCapital": 0,
   "returnOnSales": 0,
   "shareBasedCompensation": 0,
   "sellingGeneralAndAdministrativeExpense": 0,
   "shareFactor": 0,
   "shares": 0,
   "weightedAverageShares": 0,
   "weightedAverageSharesDiluted": 0,
   "salesPerShare": 0,
   "tangibleAssetValue": 0,
   "taxAssets": 0,
   "incomeTaxExpense": 0,
   "taxLiabilities": 0,
   "tangibleAssetsBookValuePerShare": 0,
   "workingCapital": 0
  }
 ]
}

*/
