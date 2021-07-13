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

package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/ergoapi/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func genmd5(str string) string {
	s := md5.New()
	s.Write([]byte(str))
	return hex.EncodeToString(s.Sum(nil))
}


func main()  {
	conf := &etcdutil.EtcdConf{
		Endpoints:   []string{"http://127.0.0.1:2379"},
	}
	conf.Rebuild()
	c, err := etcdutil.NewClient(conf)
	if err != nil {
		panic(err)
	}
	for {
		key := fmt.Sprintf("/talkcni/%v", genmd5(time.Now().Format("20060102150405")))
		putresp, err := c.Put(key, genmd5(time.Now().String()))
		if err != nil {
			panic(err)
		}
		log.Println(putresp.Header.Revision)
		log.Println("--------")
		getresp, err := c.Get(key)
		if err != nil {
			panic(err)
		}
		printkey(getresp.Kvs)
		log.Println("--------")
		getresp2, err := c.Get("/talkcni", true)
		if err != nil {
			panic(err)
		}
		printkey(getresp2.Kvs)
		log.Println("--------")
		delresp, err := c.Delete(key)
		if err != nil {
			panic(err)
		}
		log.Println("delete num:", delresp.Deleted)
		printkey(delresp.PrevKvs)
		log.Println("--------")
		// 租约
		lease := clientv3.NewLease(c.Client)
		leaseresp, err := lease.Grant(context.Background(), 4);
		if err != nil {
			panic(err)
		}
		putopresp, err :=c.PutOP(key, genmd5(key), clientv3.WithLease(leaseresp.ID))
		if err != nil {
			panic(err)
		}
		log.Println(putopresp.Header.Revision)
		log.Println("--------")
		getresp3, err := c.Get("/talkcni", true)
		if err != nil {
			panic(err)
		}
		if getresp3.Count == 0 {
			break
		}
		printkey(getresp3.Kvs)
		log.Println("--------")
		time.Sleep(2*time.Second)
	}
}

func printkey(value []*mvccpb.KeyValue) {
	for k, v := range value {
		log.Printf("key: %v, value: %v", k, v)
	}
}