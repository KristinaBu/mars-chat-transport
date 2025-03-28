package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"mars-chat-transport/entities"
	_ "mars-chat-transport/entities"
)

func WriteToKafka(segment entities.Segment) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// создание producer-а
	producer, err := sarama.NewSyncProducer([]string{KafkaAddr}, config)
	if err != nil {
		return fmt.Errorf("error creating producer: %w", err)
	}
	defer producer.Close()

	// превращение segment в сообщение для Kafka
	segmentString, _ := json.Marshal(segment)
	message := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Value: sarama.StringEncoder(segmentString),
	}

	// отправка сообщения
	_, _, err = producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}
