package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/joho/godotenv"
)

var (
	connectionString string
	queueName        string
)

const (
	maxConcurrent     = 20
	maxMessage        = 100
	ctxSessionTimeout = 5  // minute
	ctxMessageTimeout = 10 // second
)

func main() {
	loadenv()

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

	ctx3, cancel3 := context.WithTimeout(context.Background(), ctxMessageTimeout*time.Second)
	defer cancel3()

	// รับ Session Receiver สำหรับ queue
	sessionReceiver, err := client.AcceptNextSessionForQueue(ctx, queueName, nil)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Failed to accept next session: %s, reinitializing context and retrying...", err)
			return
		} else {
			log.Fatalf("Failed to accept next session: %s", err)
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
				log.Printf("Failed to receive messages: %s, reinitializing context and retrying...", err)
				break
			} else {
				log.Fatalf("Failed to receive messages: %s", err)
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
	fmt.Printf("Received message: %s\n", string(task.Body))

	// URL ที่ต้องการส่ง request ไป
	url := defineUrl(task)

	// สร้าง HTTP POST request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(task.Body))
	if err != nil {
		log.Fatalf("Failed to create POST request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// ส่ง request โดยใช้ http.Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to send POST request: %s", err)
	}
	defer resp.Body.Close()

	// อ่าน response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response body: %s", err)
	}

	// Complete ข้อความ
	err = sessionReceiver.CompleteMessage(ctx, task, nil)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Printf("Failed to complete message: %s, reinitializing context and retrying...", err)
		} else {
			log.Fatalf("Failed to complete message: %s", err)
		}
	}

	// สิ้นสุดการวัดเวลา
	elapsed := time.Since(start)

	// แสดงผล response status, body และ elapsed time
	fmt.Printf("\nResponse status: %s\n", resp.Status)
	fmt.Printf("Response body: %s\n", body)
	fmt.Printf("Elapsed time: %.2f seconds\n", elapsed.Seconds())
}

func defineUrl(task *azservicebus.ReceivedMessage) string {
	var url string

	// แปลง task.Body จาก []byte เป็น map[string]interface{}
	var data map[string]interface{}
	err := json.Unmarshal(task.Body, &data)
	if err != nil {
		log.Fatalf("Failed to unmarshal message body: %s", err)
	}

	// ตรวจสอบว่ามี key "url" ในข้อมูลหรือไม่
	if val, ok := data["url"].(string); ok {
		// fmt.Printf("Found 'url' key in message body: %s\n", val)
		url = val
	} else {
		// fmt.Println("No 'url' key found in message body")
		url = "http://localhost/api-server/api-test.php"
	}

	// Force define url for test
	// url = "http://localhost/api-server/api-test.php"

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
}
