package horizon

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	prismredis "github.com/prismgo/framework/redis"
)

const redisIntegrationEnv = "PRISMGO_REDIS_TEST_ADDR"

func TestRealRedisStoreIntegrationGate(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv(redisIntegrationEnv))
	if addr == "" {
		t.Skipf("%s is not set; skipping real Redis Horizon integration gate", redisIntegrationEnv)
	}

	ctx := context.Background()
	prefix := fmt.Sprintf("horizon_integration_real_%d", time.Now().UnixNano())
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}
	t.Cleanup(func() {
		deleteRedisIntegrationKeys(t, client, prefix)
		if err := client.Close(); err != nil {
			t.Errorf("close real Redis cleanup client: %v", err)
		}
	})

	registry := useHorizonTestContainer(t)
	redisManager, err := prismredis.NewManager(prismredis.Config{
		DefaultName: "default",
		Connections: map[string]prismredis.ConnectionConfig{
			"default": {Name: "default", Addr: addr},
		},
	})
	if err != nil {
		t.Fatalf("create real Redis manager: %v", err)
	}
	if err := registry.Instance("redis", redisManager); err != nil {
		t.Fatalf("bind real Redis manager: %v", err)
	}
	t.Cleanup(func() {
		if err := redisManager.Close(context.Background()); err != nil {
			t.Errorf("close real Redis manager: %v", err)
		}
	})

	storeA, err := NewRedisStore(RedisOptions{Connection: "default"}, StoreOptions{Prefix: prefix, HeartbeatTTL: time.Minute})
	if err != nil {
		t.Fatalf("create real Redis store A: %v", err)
	}
	storeB, err := NewRedisStore(RedisOptions{Connection: "default"}, StoreOptions{Prefix: prefix, HeartbeatTTL: time.Minute})
	if err != nil {
		t.Fatalf("create real Redis store B: %v", err)
	}
	now := time.Now().UTC()
	if err := storeA.HeartbeatSupervisor(ctx, SupervisorState{
		Name:            "supervisor-real-redis",
		Host:            "real-redis-test",
		PID:             1001,
		Status:          SupervisorRunning,
		StartedAt:       now,
		LastHeartbeatAt: now,
		Connection:      "redis",
		Queues:          []string{"default"},
	}); err != nil {
		t.Fatalf("write real Redis supervisor heartbeat: %v", err)
	}
	if err := storeA.SetGlobalPaused(ctx, true); err != nil {
		t.Fatalf("write real Redis control state: %v", err)
	}

	supervisors, err := storeB.Supervisors(ctx, now)
	if err != nil {
		t.Fatalf("read real Redis supervisors from second store: %v", err)
	}
	if len(supervisors) != 1 || supervisors[0].Name != "supervisor-real-redis" {
		t.Fatalf("real Redis supervisors = %+v, want one supervisor-real-redis", supervisors)
	}
	control, err := storeB.Control(ctx)
	if err != nil {
		t.Fatalf("read real Redis control from second store: %v", err)
	}
	if !control.GlobalPaused {
		t.Fatalf("real Redis GlobalPaused = %v, want true", control.GlobalPaused)
	}
}

func deleteRedisIntegrationKeys(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 100).Result()
		if err != nil {
			t.Errorf("scan real Redis integration keys: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete real Redis integration keys: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
