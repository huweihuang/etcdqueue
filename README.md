# Etcd Queue

> 基于 https://github.com/etcd-io/etcd/tree/main/client/v3/experimental/recipes代码修改。

Etcd Queue是基于Etcd集群做后端存储来实现任务队列的功能。

- 写入任务
- 读取任务


# 使用方法

```
package main

import (
	"fmt"

	"github.com/huweihuang/etcdqueue"
)

func main() {
	etcdConfig := &etcdqueue.EtcdConfig{
		Endpoints: "https://127.0.0.1:2379",
		CaFile:    "/etc/kubernetes/pki/etcd/ca.crt",
		KeyFile:   "/etc/kubernetes/pki/apiserver-etcd-client.key",
		CertFile:  "/etc/kubernetes/pki/apiserver-etcd-client.crt",
	}

	jobQueue, err := etcdqueue.NewEtcdQueue(etcdConfig, "/keyprefix")
	if err != nil {
		fmt.Errorf("faied to new etcd queue, error: %v", err)
	}

	err = jobQueue.Enqueue("{testjob}")
	if err != nil {
		fmt.Errorf("failed to enqueue")
	}

	job, err := jobQueue.Dequeue()
	if err != nil {
		fmt.Errorf("failed to dequeue")
	}
	fmt.Printf("queue: %v", job)
}
```
