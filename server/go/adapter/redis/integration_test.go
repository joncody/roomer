package redis_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joncody/roomer/server/go"
	redisadapter "github.com/joncody/roomer/server/go/adapter/redis"
	goredis "github.com/redis/go-redis/v9"
)

func getRedisClient(t *testing.T) goredis.UniversalClient {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := goredis.NewClient(&goredis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping live Redis integration test: Redis not reachable at %s (%v)", addr, err)
	}
	return client
}

func TestLiveRedis_TwoNodeClusterSyncAndSuppression(t *testing.T) {
	rdb := getRedisClient(t)
	defer rdb.Close()

	prefix := fmt.Sprintf("roomer:test:%d:", time.Now().UnixNano())

	// Initialize Node A and Node B
	nodeA, err := redisadapter.New(rdb,
		redisadapter.WithPrefix(prefix),
		redisadapter.WithNodeID("server_instance_A"),
	)
	if err != nil {
		t.Fatalf("failed to create node A: %v", err)
	}
	defer nodeA.Close()

	nodeB, err := redisadapter.New(rdb,
		redisadapter.WithPrefix(prefix),
		redisadapter.WithNodeID("server_instance_B"),
	)
	if err != nil {
		t.Fatalf("failed to create node B: %v", err)
	}
	defer nodeB.Close()

	var nodeAReceived int64
	var nodeBReceived int64

	// Node A subscription
	err = nodeA.Subscribe(func(room string, msg *roomer.Message) {
		atomic.AddInt64(&nodeAReceived, 1)
	})
	if err != nil {
		t.Fatalf("node A subscribe failed: %v", err)
	}

	// Node B subscription
	err = nodeB.Subscribe(func(room string, msg *roomer.Message) {
		atomic.AddInt64(&nodeBReceived, 1)
	})
	if err != nil {
		t.Fatalf("node B subscribe failed: %v", err)
	}

	// Give Redis a moment to register subscriptions
	time.Sleep(100 * time.Millisecond)

	// Node A broadcasts 500 messages
	totalMessages := 500
	ctx := context.Background()
	for i := 0; i < totalMessages; i++ {
		msg := roomer.NewMessage("lobby", "chat", "", "client_1", []byte(fmt.Sprintf("msg_%d", i)))
		if err := nodeA.Publish(ctx, "lobby", msg); err != nil {
			t.Fatalf("publish error at %d: %v", i, err)
		}
	}

	// Wait for delivery
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&nodeBReceived) == int64(totalMessages) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// VERIFICATION:
	// 1. Node B must receive all 500 messages from Node A
	receivedAtB := atomic.LoadInt64(&nodeBReceived)
	if receivedAtB != int64(totalMessages) {
		t.Errorf("Node B expected %d messages, got %d", totalMessages, receivedAtB)
	}

	// 2. Node A must receive 0 messages (loopback suppression verified)
	receivedAtA := atomic.LoadInt64(&nodeAReceived)
	if receivedAtA != 0 {
		t.Errorf("Node A received its own messages (loopback bug): expected 0, got %d", receivedAtA)
	}
}

func BenchmarkLiveRedis_PubSubThroughput(b *testing.B) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skipf("Skipping live Redis benchmark: %v", err)
	}
	defer rdb.Close()

	prefix := fmt.Sprintf("roomer:bench:%d:", time.Now().UnixNano())
	adapterA, _ := redisadapter.New(rdb, redisadapter.WithPrefix(prefix), redisadapter.WithNodeID("bench_node_A"))
	adapterB, _ := redisadapter.New(rdb, redisadapter.WithPrefix(prefix), redisadapter.WithNodeID("bench_node_B"))
	defer adapterA.Close()
	defer adapterB.Close()

	var count int64
	_ = adapterB.Subscribe(func(room string, msg *roomer.Message) {
		atomic.AddInt64(&count, 1)
	})

	time.Sleep(100 * time.Millisecond)

	msg := roomer.NewMessage("lobby", "chat", "", "bench_user", bytes.Repeat([]byte("A"), 128))
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = adapterA.Publish(ctx, "lobby", msg)
	}
}
