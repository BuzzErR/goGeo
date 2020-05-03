package main

import (
	"database/sql"
	"fmt"
	"github.com/kelvins/geocoder"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)


type Point struct {
	Lat, Lon float64
	Address string
}

type SafeUnlocated struct {
	counter int
	mux sync.Mutex
}


func (c *SafeUnlocated) Inc() {
	c.mux.Lock()
	c.counter++
	c.mux.Unlock()
}

var unlocated SafeUnlocated
var wg sync.WaitGroup

func parser(locations [][]string, key string){
	var unl int
	var coordinates []Point
	for _, location := range locations{
		geocoder.ApiKey = key
		var number int
		loc := strings.Split(location[1], "/")
		if len(loc) > 1{
			number, _ = strconv.Atoi(loc[0])
		} else {
			number, _ = strconv.Atoi(location[1])
		}
		address := geocoder.Address{
		Street:  location[0],
		Number:  number,
		City:    "Москва",
		State:   "Москва",
		Country: "Россия",
		}
		coordinate, err := geocoder.Geocoding(address)
		if err != nil {
			unl++
			unlocated.Inc()
		} else {
			address := location[0] + " " + strconv.Itoa(number)
			point := Point{coordinate.Latitude, coordinate.Longitude, address}
			coordinates = append(coordinates, point)
		}
	}
	db, err := sql.Open("sqlite3", "testGo.db")
	for err != nil{
		time.Sleep(2)
		db, err = sql.Open("sqlite3", "testGo.db")
	}
	for j := 0; j < len(coordinates); j++ {
		_, err := db.Exec("insert into LOCATIONS (name, latitude, longitude, num_of_cases)  values "+
			"($1, $2, $3, $4)", coordinates[j].Address, coordinates[j].Lat, coordinates[j].Lon, 1)
		if err != nil {
			panic(err)
		}
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
	wg.Done()
}


func main(){
	url := "https://coronavirus-control.ru/moscow-cases/"
	resp, _ := http.Get(url)
	z := html.NewTokenizer(resp.Body)
	tt := z.Next()
	flag := false
	var td []string
	var content [][]string
	data, _ := ioutil.ReadFile("apiKey.txt")
	key := string(data)
	start := time.Now()
	t := time.Now()
	start = time.Now()
	for tt != html.ErrorToken {
		switch {
		case  tt == html.StartTagToken && !flag:
			t := z.Token()
			if t.Data == "tr" {
				flag = true
			}
		case tt == html.StartTagToken && flag:
			t := z.Token()
			if t.Data == "td" {
				inner := z.Next()
				if inner == html.TextToken {
					text := (string)(z.Text())
					t := strings.TrimSpace(text)
					td = append(td, t)
				}
			}
		case tt == html.EndTagToken && flag:
			t := z.Token()
			if t.Data == "tr" {
				flag = false
				if len(td) > 1 {
					content = append(content, td[:len(td) - 1])
					td = []string{}
				}
			}
		}
		tt = z.Next()
	}
	objects_per_rout := 5
	content = content[:30]
	num_of_gorut := len(content) / objects_per_rout
	if len(content) % objects_per_rout != 0 {
		num_of_gorut += 1
	}
	fmt.Println(num_of_gorut)
	wg.Add(num_of_gorut)
	for i := 0; i < num_of_gorut; i++ {
		if (i + 1) * objects_per_rout < len(content) {
			go parser(content[i * objects_per_rout:(i + 1) * objects_per_rout], key)
		} else {
			go parser(content[i * objects_per_rout:], key)
		}
	}
	wg.Wait()
	t = time.Now()
	fmt.Println(unlocated.counter)
	fmt.Println(t.Sub(start))
}