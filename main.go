package main

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"mars-chat-transport/entities"
	"mars-chat-transport/kafka"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// запуск consumer-а
	go func() {
		if err := kafka.ReadFromKafka(); err != nil {
			fmt.Println(err)
		}
	}()
	// проверяет, собирает сегменты в сообщение
	go func() {
		ticker := time.NewTicker(entities.KafkaReadPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				kafka.ScanStorage(kafka.SendReceiveRequest)
			}
		}
	}()

	// создание роутера
	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
	http.Handle("/", r)
	r.HandleFunc("/transfer", kafka.HandleTransfer).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/send", kafka.HandleSend).Methods(http.MethodPost, http.MethodOptions)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// запуск http сервера
	srv := http.Server{
		Handler:           r,
		Addr:              ":8080",
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("Server stopped")
		}
	}()
	fmt.Println("Server started")

	// graceful shutdown
	sig := <-signalCh
	fmt.Printf("Received signal: %v\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown failed: %v\n", err)
	}
}
