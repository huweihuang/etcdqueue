// all custom code comes here

package etcdqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	spb "github.com/coreos/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

var (
	ErrKeyExists      = errors.New("key already exists")
	ErrWaitMismatch   = errors.New("unexpected wait result")
	ErrTooManyClients = errors.New("too many clients")
	ErrNoWatcher      = errors.New("no watcher channel")
	ErrKeyNotFound    = errors.New("key not found")
)

// deleteRevKey deletes a key by revision, returning false if key is missing
func deleteRevKey(kv v3.KV, key string, rev int64) (bool, error) {
	cmp := v3.Compare(v3.ModRevision(key), "=", rev)
	req := v3.OpDelete(key)
	txnresp, err := kv.Txn(context.TODO()).If(cmp).Then(req).Commit()
	if err != nil {
		return false, err
	} else if !txnresp.Succeeded {
		return false, nil
	}
	return true, nil
}

func claimFirstKey(kv v3.KV, kvs []*spb.KeyValue) (*spb.KeyValue, error) {
	for _, k := range kvs {
		ok, err := deleteRevKey(kv, string(k.Key), k.ModRevision)
		if err != nil {
			return nil, err
		} else if ok {
			return k, nil
		}
	}
	return nil, nil
}

// cancel watch
func (q *Queue) CancelWatch() {
	q.cancel()
}

// Enqueue key
func (q *Queue) EnqueueReturnKey(val string) (string, error) {
	rkv, err := newUniqueKV(q.client, q.keyPrefix, val)
	if err != nil {
		return "", err
	}
	if rkv == nil {
		return "", fmt.Errorf("newUniqueKV returns nil")
	}
	return rkv.Key(), nil
}

// Get key
func (q *Queue) GetKey(key string) (string, error) {
	resp, err := q.client.Get(q.ctx,
		strings.Join([]string{q.keyPrefix, key}, "/"),
		v3.WithLimit(1))
	if err != nil {
		return "", err
	}
	for _, k := range resp.Kvs {
		if k == nil {
			continue
		}
		zap.S().Debugf("key: %s, value: %s, create revision: %d, mod revision: %d",
			string(k.Key), string(k.Value), k.CreateRevision, k.ModRevision)
		return string(k.Value), nil
	}
	return "", ErrKeyNotFound
}

// Get key and revision
func (q *Queue) GetKeyAndRevision(key string) (string, int64, error) {
	resp, err := q.client.Get(q.ctx,
		strings.Join([]string{q.keyPrefix, key}, "/"),
		v3.WithLimit(1))
	if err != nil {
		return "", 0, err
	}
	for _, k := range resp.Kvs {
		if k == nil {
			continue
		}
		zap.S().Debugf("key: %s, value: %s, create revision: %d, mod revision: %d",
			string(k.Key), string(k.Value), k.CreateRevision, k.ModRevision)
		return string(k.Value), k.ModRevision, nil
	}
	return "", 0, ErrKeyNotFound
}

// Get all keys
func (q *Queue) GetAllKeys() (map[string]string, error) {
	results := make(map[string]string)
	resp, err := q.client.Get(q.ctx, q.keyPrefix+"/", v3.WithPrefix())
	if err != nil {
		return nil, err
	}
	zap.S().Debugf("etcd get %d kvs", len(resp.Kvs))
	for _, k := range resp.Kvs {
		if k == nil {
			continue
		}
		results[string(k.Key)] = string(k.Value)
		break
	}
	if resp.More {
		zap.S().Debug("resp.More is true")
	}
	return results, nil
}

// returns
// key string
// value string
// error
func (q *Queue) GetFirstKey() (string, string, error) {
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithFirstKey()...)
	if err != nil {
		return "", "", err
	}
	zap.S().Debugf("GetFirstKey: %d keys in queue %s", resp.Count, q.keyPrefix)

	return q.convertKey(resp)
}

// returns
// key string
// value string
// error
func (q *Queue) GetLastKey() (string, string, error) {
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithLastKey()...)
	if err != nil {
		return "", "", err
	}
	zap.S().Debugf("GetLastKey: %d keys in queue %s", resp.Count, q.keyPrefix)

	return q.convertKey(resp)
}

func (q *Queue) convertKey(resp *v3.GetResponse) (string, string, error) {
	for _, k := range resp.Kvs {
		if k == nil {
			continue
		}
		zap.S().Debugf("key: %s, value: %s, create revision: %d, mod revision: %d",
			string(k.Key), string(k.Value), k.CreateRevision, k.ModRevision)
		return string(k.Key), string(k.Value), nil
	}

	if resp.More {
		zap.S().Error("no key in resp, resp.More is true, retrying")
		return q.GetFirstKey()
	}

	// nothing yet; wait on elements
	ev, err := WaitPrefixEvents(
		q.ctx,
		q.client,
		q.keyPrefix,
		resp.Header.Revision,
		[]mvccpb.Event_EventType{mvccpb.PUT})
	if err != nil {
		return "", "", err
	}

	if ev == nil || ev.Kv == nil {
		zap.S().Error("ev | ev.Kv is nil, retrying")
		return q.GetFirstKey()
	}
	return string(ev.Kv.Key), string(ev.Kv.Value), nil
}

// returning false, nil means the key does not exist
func updateKey(kv v3.KV, key, value string) (bool, error) {
	cmp := v3.Compare(v3.CreateRevision(key), ">", 0)
	req := v3.OpPut(key, value)
	txnresp, err := kv.Txn(context.TODO()).If(cmp).Then(req).Commit()
	if err != nil {
		return false, err
	} else if !txnresp.Succeeded {
		return false, nil
	}
	return true, nil
}

// Update Key
func (q *Queue) UpdateKey(key, value string) error {
	exist, err := updateKey(q.client, key, value)
	if !exist {
		return ErrKeyNotFound
	}
	return err
}

func updateRevKey(kv v3.KV, key, value string, rev int64) (bool, error) {
	cmp := v3.Compare(v3.ModRevision(key), "=", rev)
	req := v3.OpPut(key, value)
	txnresp, err := kv.Txn(context.TODO()).If(cmp).Then(req).Commit()
	if err != nil {
		return false, err
	} else if !txnresp.Succeeded {
		return false, nil
	}
	return true, nil
}

// Update key with revision
func (q *Queue) UpdateKeyWithRevison(key, value string, revision int64) (bool, error) {
	return updateRevKey(q.client, key, value, revision)
}

// Delete key
func (q *Queue) DeleteKey(key string) error {
	_, err := q.client.Delete(q.ctx, key)
	return err
}

// Delete key with revision
func (q *Queue) DeleteKeyWithRevision(key string, revision int64) (bool, error) {
	return deleteRevKey(q.client, key, revision)
}

// Dequeue first key
func (q *Queue) DequeueFirstKey() (string, error) {
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithFirstKey()...)
	if err != nil {
		return "", err
	}

	return q.convertDequeueKey(resp)
}

func (q *Queue) convertDequeueKey(resp *v3.GetResponse) (string, error) {
	kv, err := claimFirstKey(q.client, resp.Kvs)
	if err != nil {
		return "", err
	} else if kv != nil {
		return string(kv.Value), nil
	} else if resp.More {
		// missed some items, retry to read in more
		return q.Dequeue()
	}

	// nothing yet; wait on elements
	ev, err := WaitPrefixEvents(
		q.ctx,
		q.client,
		q.keyPrefix,
		resp.Header.Revision,
		[]mvccpb.Event_EventType{mvccpb.PUT})
	if err != nil {
		return "", err
	}

	ok, err := deleteRevKey(q.client, string(ev.Kv.Key), ev.Kv.ModRevision)
	if err != nil {
		return "", err
	} else if !ok {
		return q.Dequeue()
	}
	return string(ev.Kv.Value), err
}

func putKV(kv v3.KV, key, val string, leaseID v3.LeaseID) (int64, error) {
	return 0, nil
}
