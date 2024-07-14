package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

const (
	serviceBusConnectionString = ""
	queueName                  = ""
	endpointURL                = ""
	maxConcurrentRequests      = 5
)

func main() {
	// สร้าง semaphore เพื่อจำกัดจำนวน concurrent requests
	sem := make(chan struct{}, maxConcurrentRequests)

	// สร้าง WaitGroup เพื่อรอให้ goroutines ทำงานเสร็จ
	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		// sendRequest("Message " + strconv.Itoa(i))

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// รอให้มี slot ว่างใน semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// ส่ง request ไปยัง endpoint
			sendRequest("Message " + strconv.Itoa(i))
		}(i)
	}

	// รอให้ goroutines ทำงานเสร็จ
	wg.Wait()
}

func sendRequest(payload string) {
	// fmt.Println(payload)
	sleep(payload)
}

func sleep(payload string) {
	// Seed the random number generator for better randomness. This helps ensure more unpredictable results.
	rand.Seed(time.Now().UnixNano())

	// Generate a random number between 1000 and 2000 (inclusive)
	// sleepDuration := 1000
	sleepDuration := rand.Intn(2001) + 1000

	fmt.Printf("Send %s: %d ms\n", payload, sleepDuration)
	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
	fmt.Printf("Done %s!\n", payload)
}

func sleep2() {
	rand.Seed(time.Now().UnixNano())
	sleepDuration := 100
	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
}
