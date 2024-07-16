package redis

import (
	"time"

	"github.com/go-redis/redis"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr, password string, db int) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisClient{client: rdb}
}

// ฟังก์ชันสำหรับการตั้งค่า key-value
func (r *RedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(key, value, expiration).Err()
}

// ฟังก์ชันสำหรับการดึงค่า key-value
func (r *RedisClient) Get(key string) (string, error) {
	return r.client.Get(key).Result()
}

// ฟังก์ชันสำหรับการลบค่า key
func (r *RedisClient) Del(key string) error {
	return r.client.Del(key).Err()
}

// ฟังก์ชันสำหรับการตั้งค่า hash field
func (r *RedisClient) HSet(key, field string, value interface{}) error {
	return r.client.HSet(key, field, value).Err()
}

// ฟังก์ชันสำหรับการดึงค่า hash field
func (r *RedisClient) HGet(key, field string) (string, error) {
	return r.client.HGet(key, field).Result()
}

// ฟังก์ชันสำหรับการลบค่า hash field
func (r *RedisClient) HDel(key string, fields ...string) error {
	return r.client.HDel(key, fields...).Err()
}

// ฟังก์ชันสำหรับการตรวจสอบการมีอยู่ของ hash field
func (r *RedisClient) HExists(key, field string) (bool, error) {
	return r.client.HExists(key, field).Result()
}
