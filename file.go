package main

import (
	"database/sql"
	"fmt"
	"github.com/kelvins/geocoder"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
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

func parser(locations [][]string, points chan []Point){
	var unl int
	var coordinates []Point
	for _, location := range locations{
		geocoder.ApiKey = "AIzaSyB14mkvENlG5NeR51b-YMeEV_MclVEUgXk"
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
	points <- coordinates
}


func main(){
	url := "https://coronavirus-control.ru/moscow-cases/"
	resp, _ := http.Get(url)
	z := html.NewTokenizer(resp.Body)
	tt := z.Next()
	flag := false
	var td []string
	var content [][]string
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
	num_of_gorut := 0
	points := make(chan []Point)
	content = content[:20]
	for i := 0; (i * 100) < len(content); i++ {
		if (i + 1) * 100 < len(content) {
			go parser(content[i * 100:(i + 1) * 100], points)
		} else {
			go parser(content[i:], points)
		}
		num_of_gorut++
	}
	var coordinates [][]Point
	for j:=0; j < num_of_gorut; j++ {
		coordinates = append(coordinates, <-points)
	}
	db, err := sql.Open("sqlite3", "testGo.db")
	if err != nil {
		panic(err)
	}
	for i := 0; i < num_of_gorut; i++ {
		for j := 0; j < len(coordinates[i]); j++ {
			_, err := db.Exec("insert into LOCATIONS (name, latitude, longitude, num_of_cases)  values "+
				"($1, $2, $3, $4)", coordinates[i][j].Address, coordinates[i][j].Lat, coordinates[i][j].Lon, 1)
			if err != nil {
				panic(err)
			}
		}
	}
	err = db.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println(unlocated.counter)
	t = time.Now()
	fmt.Println(t.Sub(start))
}