/*
 * Copyright (c) 2021. ysicing <i@ysicing.me>.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package etcdutil

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

const (
	defaultendpoints = "http://127.0.0.1:2379"
)

type EtcdConf struct {
	Endpoints []string `json:"endpoints,omitempty"`
	DialTimeout time.Duration `json:"timeout,omitempty"`
}

type EtcdClient struct {
	Client *clientv3.Client
}

func (conf *EtcdConf) Rebuild() {
	if len(conf.Endpoints) < 1 {
		conf.Endpoints = append(conf.Endpoints, defaultendpoints)
	}
	if conf.DialTimeout <=  5 * time.Second {
		conf.DialTimeout = 5 * time.Second
	}
}

// NewClient new etcd client
func NewClient(conf *EtcdConf) (*EtcdClient, error)  {
	config := clientv3.Config{
		Endpoints:            conf.Endpoints,
		DialTimeout:          conf.DialTimeout,
	}
	client, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}
	return &EtcdClient{Client: client}, nil
}

func (client *EtcdClient) Put(key, value string) (*clientv3.PutResponse, error) {
	kvclient := clientv3.NewKV(client.Client)
	putresp, err := kvclient.Put(context.Background(), key, value, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}
	return putresp, nil
}

func (client *EtcdClient) PutOP(key, value string, op ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	kvclient := clientv3.NewKV(client.Client)
	putresp, err := kvclient.Put(context.Background(), key, value, op...)
	if err != nil {
		return nil, err
	}
	return putresp, nil
}

func (client *EtcdClient) Get(key string, prefix ...bool) (*clientv3.GetResponse, error) {
	kvclient := clientv3.NewKV(client.Client)
	var op []clientv3.OpOption
	if len(prefix) > 0 && prefix[0] {
		op = append(op, clientv3.WithPrefix())
	}
	getresp, err := kvclient.Get(context.Background(), key, op...)
	if err != nil {
		return nil, err
	}
	return getresp, nil
}

func (client *EtcdClient) GetOP(key string, op ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	kvclient := clientv3.NewKV(client.Client)
	getresp, err := kvclient.Get(context.Background(), key, op...)
	if err != nil {
		return nil, err
	}
	return getresp, nil
}

func (client *EtcdClient) Delete(key string) ( *clientv3.DeleteResponse, error) {
	kvclient := clientv3.NewKV(client.Client)
	delresp, err := kvclient.Delete(context.Background(), key)
	if err != nil {
		return nil, err
	}
	return delresp, nil
}

func (client *EtcdClient) DeleteOP(key string, op ...clientv3.OpOption) ( *clientv3.DeleteResponse, error) {
	kvclient := clientv3.NewKV(client.Client)
	delresp, err := kvclient.Delete(context.Background(), key, op...)
	if err != nil {
		return nil, err
	}
	return delresp, nil
}