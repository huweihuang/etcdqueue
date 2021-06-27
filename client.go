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
	"fmt"
	"strings"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
)

var (
	defaultDialTimeout      = 10 * time.Second
	defaultAotuSyncInterval = 10 * time.Second
)

// EtcdConfig etcd client arguments
type EtcdConfig struct {
	Endpoints        string        // args for clientv3.Config
	DialTimeout      time.Duration // args for clientv3.Config
	AutoSyncInterval time.Duration // args for clientv3.Config
	CaFile           string        // args for clientv3.Config.TLS
	CertFile         string        // args for clientv3.Config.TLS
	KeyFile          string        // args for clientv3.Config.TLS
}

// NewETCDClient new etcd client v3
func NewETCDClient(etcdConfig *EtcdConfig) (*v3.Client, error) {
	if etcdConfig.Endpoints == "" {
		return nil, fmt.Errorf("no endpoints specified")
	}
	if etcdConfig.DialTimeout <= 10 {
		etcdConfig.DialTimeout = defaultDialTimeout
	}
	if etcdConfig.AutoSyncInterval <= 30 {
		etcdConfig.AutoSyncInterval = defaultAotuSyncInterval
	}

	config := v3.Config{
		Endpoints:            strings.Split(etcdConfig.Endpoints, ","),
		DialTimeout:          etcdConfig.DialTimeout,
		DialKeepAliveTime:    time.Second * 2,
		DialKeepAliveTimeout: time.Second * 6,
		AutoSyncInterval:     etcdConfig.AutoSyncInterval,
	}

	if etcdConfig.CaFile != "" && etcdConfig.KeyFile != "" && etcdConfig.CertFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      etcdConfig.CertFile,
			KeyFile:       etcdConfig.KeyFile,
			TrustedCAFile: etcdConfig.CaFile,
		}

		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to generate tlsConfig")
		}
		config.TLS = tlsConfig
	}

	cli, err := v3.New(config)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// NewEtcdQueue new a etcd queue
func NewEtcdQueue(etcdConfig *EtcdConfig, keyPrefix string) (*Queue, error) {
	etcdClient, err := NewETCDClient(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("faied to new etcd client")
	}
	queue := NewQueue(etcdClient, keyPrefix)
	return queue, nil
}
