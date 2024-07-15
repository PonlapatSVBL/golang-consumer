package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const (
	maxConcurrent = 5
	totalTasks    = 10
)

func main() {
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
