package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
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
	maxConcurrent = 5
	totalTasks    = 10
)

func main() {
	loadenv()
	// runConcurrentTasks()
	runQueue()
}

func loadenv() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %s", err)
	}

	connectionString = os.Getenv("CONNECTION_STRING")
	queueName = os.Getenv("QUEUE_NAME")
}

func runQueue() {
	// สร้าง Service Bus Client
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Fatalf("Failed to create client: %s", err)
	}

	// สร้าง Context สำหรับการทำงาน
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// รับ Session Receiver สำหรับ queue
	sessionReceiver, err := client.AcceptNextSessionForQueue(ctx, queueName, nil)
	if err != nil {
		log.Fatalf("Failed to accept next session: %s", err)
	}
	fmt.Printf("<===== Accept Session ID: %s =====>\n", sessionReceiver.SessionID())
	defer sessionReceiver.Close(ctx)

	// Loop เพื่อรับและประมวลผลข้อความ
	for {
		msgs, err := sessionReceiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			log.Fatalf("Failed to receive messages: %s", err)
		}

		for _, msg := range msgs {
			fmt.Printf("Received message: %s\n", string(msg.Body))

			// Complete ข้อความ
			err = sessionReceiver.CompleteMessage(ctx, msg, nil)
			if err != nil {
				log.Fatalf("Failed to complete message: %s", err)
			}
		}
	}
}

func runConcurrentTasks() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// สร้าง channel สำหรับส่งงาน
	tasks := make(chan string, totalTasks)

	// สร้าง semaphore เพื่อจำกัดจำนวน concurrent requests
	sem := make(chan struct{}, maxConcurrent)

	// สร้าง WaitGroup เพื่อรอให้ goroutines ทำงานเสร็จ
	var wg sync.WaitGroup

	// สร้าง worker goroutines
	for i := 0; i < maxConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				// รอให้มี slot ว่างใน semaphore
				sem <- struct{}{}
				// ส่ง request ไปยัง endpoint
				sendRequest(task, r)
				// ปล่อย slot ใน semaphore
				<-sem
			}
		}()
	}

	// ส่งงานไปยัง channel
	for i := 1; i <= totalTasks; i++ {
		tasks <- "Message " + strconv.Itoa(i)
	}
	close(tasks)

	// รอให้ goroutines ทำงานเสร็จ
	wg.Wait()
}

func sendRequest(payload string, r *rand.Rand) {
	sleep(payload, r)
}

func sleep(payload string, r *rand.Rand) {
	sleepDuration := r.Intn(1001) + 1000

	fmt.Printf("Send %s: %d ms\n", payload, sleepDuration)
	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	fmt.Printf("Done %s!\n", payload)
}
