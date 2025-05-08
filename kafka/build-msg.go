package kafka

import (
	"fmt"
	"mars-chat-transport/entities"
	"sync"
	"time"
)

type Storage map[time.Time]entities.Message

var (
	storage = Storage{}
	mu      = &sync.Mutex{} // Общий мьютекс
)

func addMessage(segment entities.Segment) {
	storage[segment.SendTime] = entities.Message{
		Received: 0,
		Total:    segment.TotalSegments,
		Last:     time.Now().UTC(),
		Username: segment.Username,
		Segments: make([]string, segment.TotalSegments), // заранее выделяем память, это важно!
	}
}

func AddSegment(segment entities.Segment) {
	// используем мьютекс, чтобы избежать конкуретного доступа к хранилищу
	mu.Lock()
	defer mu.Unlock()

	// если это первый сегмент сообщения, создаем пустое сообщение
	sendTime := segment.SendTime
	_, found := storage[sendTime]
	if !found {
		addMessage(segment)
	}

	// добавляем в сообщение информацию о сегменте
	message, _ := storage[sendTime]
	message.Received++
	message.Last = time.Now().UTC()
	message.Segments[segment.SegmentNumber-1] = segment.SegmentPayload // сохраняем правильный порядок сегментов
	storage[sendTime] = message
}

func getMessageText(sendTime time.Time) string {
	result := ""
	message, _ := storage[sendTime]
	for _, segment := range message.Segments {
		result += segment
	}
	return result
}

type sendFunc func(body entities.ReceiveRequest)

func ScanStorage(sender sendFunc) {
	mu.Lock()
	defer mu.Unlock()

	payload := entities.ReceiveRequest{}
	for sendTime, message := range storage {
		if message.Received == message.Total { // если пришли все сегменты
			payload = entities.ReceiveRequest{
				Username: message.Username,
				Text:     getMessageText(sendTime), // склейка сообщения
				SendTime: sendTime,
				Error:    "",
			}
			fmt.Printf("sent message: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		} else if time.Since(message.Last) > entities.KafkaReadPeriod+time.Second { // если канальный уровень потерял сегмент
			payload = entities.ReceiveRequest{
				Username: message.Username,
				Text:     "",
				SendTime: sendTime,
				Error:    entities.SegmentLostError, // ошибка
			}
			fmt.Printf("sent error: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		}
	}
}
