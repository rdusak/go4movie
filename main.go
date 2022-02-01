package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

type WriteJob struct {
	OP     int
	writer http.ResponseWriter
	req    http.Request
}

var writeJobs chan WriteJob
var wg sync.WaitGroup
var workerCount int = 10
var db *sql.DB

type Movie struct {
	ID       int     `json:"id"`
	Title    string  `json:"title"`
	Director string  `json:"director"`
	Price    float64 `json:"price"`
}

func elog(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func getMovies(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	var movies []Movie
	rows, err := db.Query("select id, title, director, price from movies")
	elog(err)
	defer rows.Close()
	for rows.Next() {
		var movie Movie
		err = rows.Scan(&movie.ID, &movie.Title, &movie.Director, &movie.Price)
		elog(err)
		movies = append(movies, movie)
	}
	elog(rows.Err())
	json.NewEncoder(w).Encode(movies)
}

func getMovie(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	elog(err)
	stmt, err := db.Prepare("select id, title, director, price from movies where id = ?")
	elog(err)
	defer stmt.Close()
	var movie Movie
	err = stmt.QueryRow(strconv.Itoa(id)).Scan(&movie.ID, &movie.Title, &movie.Director, &movie.Price)
	if err == sql.ErrNoRows {
		json.NewEncoder(w).Encode(&Movie{})
	} else {
		elog(err)
		json.NewEncoder(w).Encode(movie)
	}
}


func createMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var movie Movie
	_ = json.NewDecoder(r.Body).Decode(&movie)
	tx, err := db.Begin()
	elog(err)
	stmt, err := tx.Prepare("insert into movies(title, director, price) values(?, ?, ?)")
	elog(err)
	defer stmt.Close()
	_, err = stmt.Exec(movie.Title, movie.Director, movie.Price)
	elog(err)
	tx.Commit()
	defer wg.Done()
	json.NewEncoder(w).Encode(movie)
}

func updateMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	var movie Movie
	_ = json.NewDecoder(r.Body).Decode(&movie)
	id, err := strconv.Atoi(params["id"])
	elog(err)
	stmt, err := db.Prepare("update movies set title = ?, director = ?, price = ? where id = ?")
	elog(err)
	defer stmt.Close()
	_, err = stmt.Exec(movie.Title, movie.Director, movie.Price, id)
	elog(err)
	defer wg.Done()
	json.NewEncoder(w).Encode(movie)
}

func deleteMovie(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	var movie Movie
	id, err := strconv.Atoi(params["id"])
	elog(err)
	stmt, err := db.Prepare("delete from movies where id = ?")
	elog(err)
	defer stmt.Close()
	defer wg.Done()
	_, err = stmt.Exec(id)
	elog(err)
	json.NewEncoder(w).Encode(movie)
}

func createMovieHandler(w http.ResponseWriter, r *http.Request) {
	wj := WriteJob{
		OP:      1,
		writer:  w,
		req:    *r,
	}
	writeJobs <- wj
	wg.Add(1)
    wg.Wait()
}
func updateMovieHandler(w http.ResponseWriter, r *http.Request) {
	wj := WriteJob{
		OP:      2,
		writer:  w,
		req:    *r,
	}
	writeJobs <- wj
	wg.Add(1)
    wg.Wait()
}
func deleteMovieHandler(w http.ResponseWriter, r *http.Request) {
	wj := WriteJob{
		OP:      3,
		writer:  w,
		req:    *r,
	}
	writeJobs <- wj
	wg.Add(1)
    wg.Wait()
}

func writeWorker() {
	for {
		wj := <-writeJobs
		if wj.OP == 1 {
			createMovie(wj.writer, &wj.req)
		} else if wj.OP == 2 {
			updateMovie(wj.writer, &wj.req)
		} else if wj.OP == 3 {
			deleteMovie(wj.writer, &wj.req)
		}
		time.Sleep(2 * time.Second)
	}
}

func main() {

	db, _ = sql.Open("sqlite3", "file:moviestore.db?cache=shared&mode=memory")
	dbsetup(db)
	defer db.Close()
	writeJobs = make(chan WriteJob, 100)
	r := mux.NewRouter()

	r.HandleFunc("/", getMovies).Methods("GET")
	r.HandleFunc("/{id}", getMovie).Methods("GET")
	r.HandleFunc("/", createMovieHandler).Methods("POST")
	r.HandleFunc("/{id}", updateMovieHandler).Methods("PUT")
	r.HandleFunc("/{id}", deleteMovieHandler).Methods("DELETE")

	for i := 0; i < workerCount; i++ {
		go writeWorker()
	}

	log.Fatal(http.ListenAndServe(":8080", r))
}

func dbsetup(db *sql.DB) {
	os.Remove("./moviestore.db")

	sqlStmt := `
	create table movies (
						id integer primary key, 
						title text,
						director text,
						price real
						);
	delete from movies;
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	tx, err := db.Begin()
	elog(err)
	stmt, err := tx.Prepare("insert into movies(id, title, director, price) values(?, ?, ?, ?)")
	elog(err)
	defer stmt.Close()
	_, err = stmt.Exec(1, "The Princess Bride", "Rob Reiner", 39.99)
	elog(err)
	_, err = stmt.Exec(2, "Hot Fuzz", "Edgar Wright", 28.99)
	elog(err)
	_, err = stmt.Exec(3, "Airplane!", "Jim Abrahams", 17.99)
	elog(err)
	_, err = stmt.Exec(4, "Dracula: Dead and Loving It", "Mel Brooks", 34.99)
	elog(err)
	_, err = stmt.Exec(5, "Hot Shots!", "Jim Abrahams", 54.99)
	elog(err)
	tx.Commit()
}
