package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mars-chat-transport/entities"
	"math"
	"net/http"
)

const CodeUrl = "http://172.20.10.5:3050/code" // адрес канального уровня

func SplitMessage(payload string, segmentSize int) []string {
	result := make([]string, 0)

	length := len(payload) // длина в байтах
	segmentCount := int(math.Ceil(float64(length) / float64(segmentSize)))

	for i := 0; i < segmentCount; i++ {
		result = append(result, payload[i*segmentSize:min((i+1)*segmentSize, length)]) // срез делается также по байтам
	}

	return result
}

func SendSegment(body entities.Segment) {
	reqBody, _ := json.Marshal(body)
	fmt.Printf("Отправка на канальный: %s\n", string(reqBody))

	req, _ := http.NewRequest("POST", CodeUrl, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	fmt.Printf("SendSegment: %+v\n", body)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("ОШИБКА отправки на канальный: %v\n", err)
		return
	}
	fmt.Printf("Ответ канального: %d\n", resp.StatusCode)

	defer resp.Body.Close()

}

func HandleSend(w http.ResponseWriter, r *http.Request) {
	// читаем тело запроса - сообщение
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// парсим сообщение в структуру
	message := entities.SendRequest{}
	if err = json.Unmarshal(body, &message); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Printf("Send!!!", message)

	// разбиваем текст сообщения на сегменты
	segments := SplitMessage(message.Text, entities.SegmentSize)
	total := len(segments)

	// в цикле отправляем сегменты на канальный уровень
	for i, segment := range segments {
		payload := entities.Segment{
			SegmentNumber:  i + 1,
			TotalSegments:  total,
			Username:       message.Username,
			SendTime:       message.SendTime,
			SegmentPayload: segment,
		}
		go SendSegment(payload) // запускаем горутину с отправкой на канальный уровень, не будем дожидаться результата ее выполнения
		fmt.Printf("sent segment: %+v\n", payload)
	}

}

func SendReceiveRequest(body entities.ReceiveRequest) {
	reqBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", entities.ReceiveUrl, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	fmt.Printf("Прикладной!!!")
	defer resp.Body.Close()
}
