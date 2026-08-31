package presence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const claimScript = `
local old = redis.call('GET', KEYS[1])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return old`
const renewScript = `
local old = redis.call('GET', KEYS[1])
if not old then return 0 end
local value = cjson.decode(old)
if value.lease_token ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1`
const releaseScript = `
local old = redis.call('GET', KEYS[1])
if not old then return 0 end
local value = cjson.decode(old)
if value.lease_token ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1`

type RedisRegistry struct {
	client redis.UniversalClient
	prefix string
	ttl    time.Duration
}

func NewRedisRegistry(client redis.UniversalClient, prefix string, ttl time.Duration) *RedisRegistry {
	if prefix == "" {
		prefix = "game-gateway:presence"
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &RedisRegistry{client: client, prefix: prefix, ttl: ttl}
}
func (r *RedisRegistry) key(userID string) string { return r.prefix + ":user:" + userID }
func (r *RedisRegistry) channel() string          { return r.prefix + ":evict" }
func (r *RedisRegistry) Claim(ctx context.Context, owner Owner) (*Owner, error) {
	if !owner.Valid() {
		return nil, fmt.Errorf("invalid presence owner")
	}
	raw, err := json.Marshal(owner)
	if err != nil {
		return nil, err
	}
	result, err := r.client.Eval(ctx, claimScript, []string{r.key(owner.UserID)}, raw, r.ttl.Milliseconds()).Text()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim presence: %w", err)
	}
	var old Owner
	if err := json.Unmarshal([]byte(result), &old); err != nil {
		return nil, fmt.Errorf("decode previous owner: %w", err)
	}
	return &old, nil
}
func (r *RedisRegistry) Renew(ctx context.Context, owner Owner) (bool, error) {
	return r.conditional(ctx, renewScript, owner, r.ttl.Milliseconds())
}
func (r *RedisRegistry) Release(ctx context.Context, owner Owner) (bool, error) {
	return r.conditional(ctx, releaseScript, owner, 0)
}
func (r *RedisRegistry) conditional(ctx context.Context, script string, owner Owner, ttlMS int64) (bool, error) {
	result, err := r.client.Eval(ctx, script, []string{r.key(owner.UserID)}, owner.LeaseToken, ttlMS).Int64()
	if err != nil {
		return false, fmt.Errorf("conditional presence operation: %w", err)
	}
	return result == 1, nil
}
func (r *RedisRegistry) PublishEviction(ctx context.Context, target Owner) error {
	raw, err := json.Marshal(target)
	if err != nil {
		return err
	}
	return r.client.Publish(ctx, r.channel(), raw).Err()
}
func (r *RedisRegistry) Subscribe(ctx context.Context, handler func(Owner)) error {
	pubsub := r.client.Subscribe(ctx, r.channel())
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}
	for {
		message, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		var target Owner
		if json.Unmarshal([]byte(message.Payload), &target) == nil && target.Valid() {
			handler(target)
		}
	}
}
