package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	redis "github.com/PonlapatSVBL/golang-consumer/libs"
	"github.com/joho/godotenv"
)

var (
	redisClient *redis.RedisClient
)

var (
	connectionString string
	queueName        string
	redisHost        string
	redisPass        string
)

const (
	maxConcurrent             = 5
	maxMessage                = 100
	ctxSessionTimeout         = 5  // minute
	ctxMessageTimeout         = 10 // second
	ctxCompleteMessageTimeout = 5  // minute
)

func main() {
	loadenv()
	// สร้าง RedisClient
	redisClient = redis.NewRedisClient(redisHost, redisPass, 0)

	// สร้าง Service Bus Client
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Fatalf("Failed to create client: %s", err)
	}

	for {
		fmt.Println("run...")
		receiveMessageQueue(client)
	}
}

func receiveMessageQueue(client *azservicebus.Client) {
	// สร้าง Context สำหรับการทำงาน
	ctx, cancel := context.WithTimeout(context.Background(), ctxSessionTimeout*time.Minute)
	defer cancel()

	ctx2, cancel2 := context.WithTimeout(context.Background(), ctxMessageTimeout*time.Second)
	defer cancel2()

	ctx3, cancel3 := context.WithTimeout(context.Background(), ctxCompleteMessageTimeout*time.Minute)
	defer cancel3()

	// รับ Session Receiver สำหรับ queue
	sessionReceiver, err := client.AcceptNextSessionForQueue(ctx, queueName, nil)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Failed to accept next session: %s, reinitializing context and retrying...\n", err)
			return
		} else {
			// log.Fatalf("Failed to accept next session: %s", err)
			log.Printf("Failed to accept next session: %s\n", err)
			return
		}
	}
	fmt.Printf("<===== Accept Session ID: %s =====>\n", sessionReceiver.SessionID())
	defer sessionReceiver.Close(ctx)

	// Loop เพื่อรับและประมวลผลข้อความ
	for {
		msgs, err := sessionReceiver.ReceiveMessages(ctx2, maxMessage, nil)
		// fmt.Println("have messages.")
		if err != nil {
			if ctx2.Err() == context.DeadlineExceeded {
				log.Printf("Failed to receive messages: %s, reinitializing context and retrying...\n", err)
				break
			} else {
				// log.Fatalf("Failed to receive messages: %s", err)
				log.Printf("Failed to receive messages: %s\n", err)
				break
			}
		}

		processMessages(ctx3, sessionReceiver, msgs)
	}
}

func processMessages(ctx context.Context, sessionReceiver *azservicebus.SessionReceiver, msgs []*azservicebus.ReceivedMessage) {
	// สร้าง channel สำหรับส่งงาน
	tasks := make(chan *azservicebus.ReceivedMessage, len(msgs))

	// สร้าง semaphore เพื่อจำกัดจำนวน concurrent requests
	sem := make(chan int, maxConcurrent)

	// สร้าง WaitGroup เพื่อรอให้ goroutines ทำงานเสร็จ
	var wg sync.WaitGroup

	// สร้าง worker goroutines
	for i := 0; i < maxConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				// รอให้มี slot ว่างใน semaphore
				sem <- 1

				// ส่ง request ไปยัง endpoint
				postRequest(ctx, sessionReceiver, task)

				// ปล่อย slot ใน semaphore
				<-sem
			}
		}()
	}

	// ส่งงานไปยัง channel
	for _, msg := range msgs {
		tasks <- msg
	}
	close(tasks)

	wg.Wait()
}

func postRequest(ctx context.Context, sessionReceiver *azservicebus.SessionReceiver, task *azservicebus.ReceivedMessage) {
	// เริ่มการวัดเวลา
	start := time.Now()

	// Printf task.Body จาก []byte เป็น string
	fmt.Printf("Received message: %s\n\n", string(task.Body))

	// URL ที่ต้องการส่ง request ไป
	url := defineUrl(task)

	// สร้าง HTTP POST request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(task.Body))
	if err != nil {
		// log.Fatalf("Failed to create POST request: %s", err)
		log.Printf("Failed to create POST request: %s\n", err)
		return
	}

	// เพิ่ม header ใน request
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Consumer-Key", "ZISgIXNxsDkOFanW0xcr")

	// ส่ง request โดยใช้ http.Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// log.Fatalf("Failed to send POST request: %s", err)
		log.Printf("Failed to send POST request: %s\n", err)
		return
	}
	defer resp.Body.Close()

	// อ่าน response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// log.Fatalf("Failed to read response body: %s", err)
		log.Printf("Failed to read response body: %s\n", err)
		return
	}

	// Complete ข้อความ
	err = sessionReceiver.CompleteMessage(ctx, task, nil)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Failed to complete message: %s, reinitializing context and retrying...\n", err)
		} else {
			// log.Fatalf("Failed to complete message: %s", err)
			log.Printf("Failed to complete message: %s\n", err)
			return
		}
	}

	// Dequeue employee from redis
	var payload map[string]interface{}
	err = json.Unmarshal(task.Body, &payload)
	if err != nil {

	} else {
		instanceServerChannelId := ""
		if encodedId, ok := payload["instance_server_channel_id"].(string); ok {
			decodedId, err := base64.StdEncoding.DecodeString(encodedId)
			if err != nil {
				fmt.Printf("Failed to decode instance_server_channel_id: %s\n", err)
			} else {
				instanceServerChannelId = string(decodedId)
			}
		}
		key := instanceServerChannelId + "_PAYROLL_QUEUE"

		field := ""
		if encodedField, ok := payload["employee_id"].(string); ok {
			decodedField, err := base64.StdEncoding.DecodeString(encodedField)
			if err != nil {
				fmt.Printf("Failed to decode employee_id: %s\n", err)
			} else {
				field = string(decodedField)
			}
		}

		_ = redisClient.HDel(key, field)
	}

	// สิ้นสุดการวัดเวลา
	elapsed := time.Since(start)

	// แสดงผล response status, body และ elapsed time
	fmt.Printf("Response status: %s\n", resp.Status)
	fmt.Printf("Response body: %s\n", body)
	fmt.Printf("Elapsed time: %.2f seconds\n\n", elapsed.Seconds())
}

func defineUrl(task *azservicebus.ReceivedMessage) string {
	var url string

	// แปลง task.Body จาก []byte เป็น map[string]interface{}
	var data map[string]interface{}
	err := json.Unmarshal(task.Body, &data)
	if err != nil {
		// log.Fatalf("Failed to unmarshal message body: %s", err)
		log.Printf("Failed to unmarshal message body: %s\n", err)
	}

	// ตรวจสอบว่ามี key "url" ในข้อมูลหรือไม่
	if val, ok := data["url"].(string); ok {
		// fmt.Printf("Found 'url' key in message body: %s\n", val)
		url = val
	} else {
		// fmt.Println("No 'url' key found in message body")
		// url = "http://localhost/api-server/api-test.php"
		url = "https://core.humansoft.co.th/api-web.php"
	}

	// Force define url for test
	// url = "http://localhost/api-server/api-web.php"

	return url
}

/* func sleep() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sleepDuration := r.Intn(1001) + 100
	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
} */

func loadenv() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	connectionString = os.Getenv("CONNECTION_STRING")
	queueName = os.Getenv("QUEUE_NAME")
	redisHost = os.Getenv("REDIS_HOST")
	redisPass = os.Getenv("REDIS_PASS")
}
