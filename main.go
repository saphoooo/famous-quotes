package main

import (
	"context"
	_ "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/lib/pq"
	sqltrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/database/sql"
	redigotrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/gomodule/redigo"
	muxtrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/gorilla/mux"
	gormtrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/jinzhu/gorm"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	logFile, err := os.OpenFile("logfile.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	tracer.Start(
		tracer.WithGlobalTag("env", "dev"),
		tracer.WithGlobalTag("service", "famous-quotes"),
		tracer.WithGlobalTag("version", "v0.0.1"),
		tracer.WithRuntimeMetrics(),
		tracer.WithPeerServiceDefaults(true),
		tracer.WithGlobalServiceName(true),
		// tracer.WithPeerServiceMapping("redis", "famous-quotes-redis"),
		// tracer.WithPeerServiceMapping("postgres.db", "famous-quotes-postgres.db"),
	)
	defer tracer.Stop()

	r := muxtrace.NewRouter(muxtrace.WithServiceName("famous-quotes"))
	r.HandleFunc("/api/r", random).Methods("GET")
	r.HandleFunc("/api/word", word).Methods("POST")
	log.Print("INFO", "Start listening on :8000...")
	err = http.ListenAndServe(":8000", r)
	if err != nil {
		log.Fatal("ERROR", err)
	}
}

func random(w http.ResponseWriter, r *http.Request) {
	span, _ := tracer.StartSpanFromContext(r.Context(), "famous-quotes.r")
	defer span.Finish()

	var QuoteContent string
	QuoteContent, err := queryRandomQuote(span.Context())
	if err != nil {
		log.Println("ERROR", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), err.Error())
	}
	log.Println("INFO", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), "yippee, that works...")
	fmt.Fprint(w, QuoteContent)
}

func word(w http.ResponseWriter, r *http.Request) {
	span, _ := tracer.StartSpanFromContext(r.Context(), "famous-quotes.word")
	defer span.Finish()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("ERROR", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "oops something wrong hapenned...")
		return
	}
	log.Println("INFO", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), "wow, it works perfectly...")

	msg := message{}
	err = json.Unmarshal(body, &msg)
	if err != nil {
		log.Println("ERROR", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "oops something wrong hapenned...")
		return
	}

	if msg.QueryValue == "onion" {
		err := fmt.Sprintf("%d:%d: syntax error", 194, 18)
		log.Println("ERROR", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "oops something wrong hapenned...")
		return
	}

	pool := &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redigotrace.Dial("tcp", "localhost:6379",
				redigotrace.WithServiceName("famous-quotes-redis"),
			)
		},
	}

	conn := pool.Get()
	defer conn.Close()

	if QuoteContent, ok := getCachedData(conn, span.Context(), msg.QueryValue); ok {
		log.Println("INFO", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), "content found in redis...")
		quote := reply{Quote: QuoteContent}
		req, err := json.Marshal(quote)
		if err != nil {
			log.Println("ERROR", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "oops something wrong hapenned...")
			return
		}
		log.Println("INFO", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), "okay, you rocks...")
		w.WriteHeader(http.StatusFound)
		fmt.Fprint(w, string(req))
		return
	}

	var QuoteContent string
	QuoteContent, err = queryQuote(msg.QueryValue, span.Context(), conn)
	if err != nil {
		log.Println("ERROR", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), err.Error())
		w.WriteHeader(http.StatusNotModified)
		fmt.Fprintf(w, "oops something wrong hapenned...")
		return
	}
	log.Println("INFO", "span_id "+strconv.FormatUint(span.Context().SpanID(), 10), "trace_id "+strconv.FormatUint(span.Context().TraceID(), 10), "everything went amazingly well...")

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, QuoteContent)
}

func getCachedData(conn redis.Conn, spanctx ddtrace.SpanContext, keyword string) (string, bool) {

	span, ctx := tracer.StartSpanFromContext(context.Background(), "getCachedData", tracer.ServiceName("famous-quotes-redis"), tracer.ChildOf(spanctx))
	defer span.Finish()

	jsonData, err := redis.Values(conn.Do("LRANGE", keyword, "0", "-1", ctx))
	if err != nil {
		return "", false
	}

	var result []Word
	for _, data := range jsonData {
		var row Word

		// Parse the JSON data into the struct
		err := json.Unmarshal(data.([]byte), &row)
		if err != nil {
			return "", false
		}

		result = append(result, row)
	}
	if len(result) == 0 {
		return "", false
	}
	return result[rand.Intn(len(result))].QUOTE, true
}

func cacheData(data []Word, conn redis.Conn, spanctx ddtrace.SpanContext, keyword string) error {

	span, ctx := tracer.StartSpanFromContext(context.Background(), "cacheData", tracer.ServiceName("famous-quotes-redis"), tracer.ChildOf(spanctx))
	defer span.Finish()

	for _, row := range data {
		jsonData, err := json.Marshal(row)
		if err != nil {
			return err
		}

		_, err = redis.Int(conn.Do("LPUSH", keyword, jsonData, ctx))
		if err != nil {
			return err
		}

	}

	_, err := redis.Int(conn.Do("EXPIRE", keyword, 5, ctx))
	if err != nil {
		return err
	}

	return nil
}

func queryQuote(keyword string, spanctx ddtrace.SpanContext, conn redis.Conn) (quote string, err error) {
	span, ctx := tracer.StartSpanFromContext(context.Background(), "func.queryQuote", tracer.ChildOf(spanctx))
	defer span.Finish()

	psqlInfo := "host=localhost port=5432 user=postgres password=datadog101 dbname=quotes sslmode=disable"
	sqltrace.Register("postgres", &pq.Driver{}, sqltrace.WithDBMPropagation(tracer.DBMPropagationModeFull))
	db, err := gormtrace.Open("postgres", psqlInfo, gormtrace.WithServiceName("famous-quotes-postgres.db"))
	db = gormtrace.WithContext(ctx, db)
	if err != nil {
		return "", err
	}
	defer db.Close()

	var word []Word
	db.Where("keyword LIKE ?", "%"+keyword+"%").Find(&word)

	if len(word) >= 1 {
		err = cacheData(word, conn, span.Context(), keyword)
		if err != nil {
			return "", err
		}

		return word[rand.Intn(len(word))].QUOTE, nil
	}
	return "", errors.New("oops, sorry but there is no quote associated with the word " + keyword)
}

func queryRandomQuote(spanctx ddtrace.SpanContext) (quote string, err error) {
	span, ctx := tracer.StartSpanFromContext(context.Background(), "func.queryRandomQuote", tracer.ChildOf(spanctx))
	defer span.Finish()

	psqlInfo := "host=localhost port=5432 user=postgres password=datadog101 dbname=quotes sslmode=disable"
	sqltrace.Register("postgres", &pq.Driver{}, sqltrace.WithDBMPropagation(tracer.DBMPropagationModeFull))
	db, err := gormtrace.Open("postgres", psqlInfo, gormtrace.WithServiceName("famous-quotes-postgres.db"))
	db = gormtrace.WithContext(ctx, db)
	if err != nil {
		return "", err
	}
	defer db.Close()

	var word []Word
	result := db.Find(&word)
	if result.Error != nil {
		return "", result.Error
	}

	return word[rand.Intn(len(word))].QUOTE, nil
}

type message struct {
	QueryValue string `json:"queryValue"`
}

type reply struct {
	Quote string `json:"quote"`
}

type Word struct {
	ID      uint
	KEYWORD string
	QUOTE   string
}
