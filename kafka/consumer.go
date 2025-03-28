package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"mars-chat-transport/entities"
	_ "mars-chat-transport/entities"
)

const (
	KafkaAddr  = "localhost:29092"
	KafkaTopic = "segments"
)

func ReadFromKafka() error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// создание consumer-а
	consumer, err := sarama.NewConsumer([]string{KafkaAddr}, config)
	if err != nil {
		return fmt.Errorf("error creating consumer: %w", err)
	}
	defer consumer.Close()

	// подключение consumer-а к топика
	partitionConsumer, err := consumer.ConsumePartition(KafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("error opening topic: %w", err)
	}
	defer partitionConsumer.Close()

	// бесконечный цикл чтения
	for {
		select {
		case message := <-partitionConsumer.Messages():
			segment := entities.Segment{}
			if err := json.Unmarshal(message.Value, &segment); err != nil {
				fmt.Printf("Error reading from kafka: %v", err)
			}
			fmt.Printf("%+v\n", segment) // выводим в консоль прочитанный сегмент
			AddSegment(segment)
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %s\n", err.Error())
		}
	}
}
