package entities

import "time"

const SegmentSize = 100
const ReceiveUrl = "http://hohoho:3000/receive" // адрес websocket-сервера прикладного уровня

type SendRequest struct {
	Id       int       `json:"id,omitempty"`
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
}

type Segment struct {
	SegmentNumber  int       `json:"segment_number"`
	TotalSegments  int       `json:"total_segments"`
	Username       string    `json:"username"`
	SendTime       time.Time `json:"send_time"`
	SegmentPayload string    `json:"payload"`
}

type Message struct {
	Received int
	Total    int
	Last     time.Time
	Username string
	Segments []string
}

const (
	SegmentLostError = "lost"
	KafkaReadPeriod  = 2 * time.Second
)

// структура тела запроса на прикладной уровень
type ReceiveRequest struct {
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
	Error    string    `json:"error"`
}
