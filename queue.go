// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdqueue

import (
	"context"

	v3 "github.com/coreos/etcd/clientv3"
)

// Queue implements a multi-reader, multi-writer distributed queue.
type Queue struct {
	client *v3.Client
	ctx    context.Context
	cancel context.CancelFunc

	keyPrefix string
}

// NewQueue create Queue
func NewQueue(client *v3.Client, keyPrefix string) *Queue {
	ctx1, cancel := context.WithCancel(context.Background())
	return &Queue{client, ctx1, cancel, keyPrefix}
}

// enqueue
func (q *Queue) Enqueue(val string) error {
	_, err := newUniqueKV(q.client, q.keyPrefix, val)
	return err
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the
// queue is empty, Dequeue blocks until elements are available.
func (q *Queue) Dequeue() (string, error) {
	// TODO: fewer round trips by fetching more than one key
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithFirstRev()...)
	if err != nil {
		return "", err
	}

	return q.convertDequeueKey(resp)
}
