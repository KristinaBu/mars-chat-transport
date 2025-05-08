package kafka

import (
	"encoding/json"
	"fmt"
	"io"
	"mars-chat-transport/entities"
	"net/http"
)

func HandleTransfer(w http.ResponseWriter, r *http.Request) {
	// читаем тело запроса - сегмент
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// парсим сегмент в структуру
	segment := entities.Segment{}
	if err = json.Unmarshal(body, &segment); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// пишем сегмент в Kafka
	if err = WriteToKafka(segment); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	fmt.Printf("Канальный!!!")

	w.WriteHeader(http.StatusOK)
}
