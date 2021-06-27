package etcdqueue

import (
	"testing"
)

var (
	jobQueue *Queue
)

func TestSetup(t *testing.T) {
	var err error
	etcdConfig := &EtcdConfig{
		Endpoints: "https://127.0.0.1:2379",
		CaFile:    "/etc/kubernetes/pki/etcd/ca.crt",
		KeyFile:   "/etc/kubernetes/pki/apiserver-etcd-client.key",
		CertFile:  "/etc/kubernetes/pki/apiserver-etcd-client.crt",
	}

	jobQueue, err = NewEtcdQueue(etcdConfig, "/keyprefix")
	if err != nil {
		t.Errorf("faied to new etcd queue, error: %v", err)
	}
}

func TestQueue_Enqueue(t *testing.T) {
	TestSetup(t)
	err := jobQueue.Enqueue("{testjob}")
	if err != nil {
		t.Errorf("failed to enqueue")
	} else {
		t.Logf("enqueue succeed")
	}
}

func TestQueue_Dequeue(t *testing.T) {
	TestSetup(t)
	job, err := jobQueue.Dequeue()
	if err != nil {
		t.Errorf("failed to dequeue")
	} else {
		t.Logf("dequeue succeed, queue: %v", job)
	}
}
