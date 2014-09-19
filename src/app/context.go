package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"redis"
)

func help(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	c := context.c

	for cmd := range c.handlers {
		result = result + cmd + "\n"
	}
	return
}

func procs(ud interface{}, args []string) (result string, err error) {
	count := 0
	if len(args) > 0 {
		if count, err = strconv.Atoi(args[0]); err != nil {
			Error("illegal parameter: %v", err)
			return
		}
	}
	old := runtime.GOMAXPROCS(count)
	if count < 1 {
		result = fmt.Sprintf("max procs:%d", old)
	} else {
		result = fmt.Sprintf("modify max procs:%d -> %d", old, count)
	}
	return
}

func shutdown(ud interface{}, args []string) (result string, err error) {
	passwd := ""
	if len(args) > 0 {
		passwd = args[0]
	}

	if passwd != "confirm" {
		err = errors.New("wrong password")
		return
	}

	context := ud.(*Context)
	context.c.Stop()
	context.m.Stop()
	context.s.Stop()
	context.quit_chan <- true
	result = "done"
	return
}

func info(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db

	key := ""
	if len(args) > 0 {
		key = args[0]
	}
	result = db.Info(key)
	return
}

func sync_one(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	sync_queue := context.sync_queue

	key := ""
	if len(args) > 0 {
		key = args[0]
	}
	sync_queue <- key
	return
}

func sync_all(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	sync_queue := context.sync_queue

	cli := context.redis
	all_key_strings, err := cli.Exec("keys", "*")
	if err != nil {
		Error("sync_all cmd service failed:%v", err)
		return
	}
	keys := all_key_strings.([]string)
	sort.Strings(keys)
	sz := len(keys)
	cur := 0
	for _, key := range keys {
		sync_queue <- key
		cur += 1
		if cur%100 == 0 {
			Info("sync progress: %d/%d, queue:%d", cur, sz, len(sync_queue))
		}
	}
	Info("sync finish: %d/%d", cur, sz)
	result = strconv.Itoa(sz)
	return
}

func count(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	defer it.Close()

	i := 0
	for it.Seek(INDEX_KEY_START); it.Valid() && bytes.Compare(it.Key(), INDEX_KEY_END) <= 0; it.Next() {
		i++
	}
	result = strconv.Itoa(i)
	return
}

func check(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	cli := context.redis
	it := db.NewIterator()
	defer it.Close()

	count := 0
	mismatch_count := 0
	all_key_strings, err := cli.Exec("keys", "*")
	redis_key_count := len(all_key_strings.([]string))
	for it.SeekToFirst(); it.Valid(); it.Next() {
		var leveldb_data map[string]string
		if json_err := json.Unmarshal(it.Value(), &leveldb_data); json_err != nil {
			Error("json unmarshal err:%v", json_err)
			continue
		}
		redis_data := make(map[string]string)
		err_redis := cli.Hgetall(string(it.Key()), redis_data)
		if err_redis != nil {
			Error("redis err:%v", err_redis)
			continue
		}
		if len(redis_data) != len(leveldb_data) {
			Error("k/v amount mismatch:%v -> %d vs %d", string(it.Key()), len(redis_data), len(leveldb_data))
			mismatch_count++
			count++
			continue
		}
		for key, value := range redis_data {
			if value != leveldb_data[key] {
				Error("key mismatch:%v", string(it.Key()))
				mismatch_count++
				break
			}
		}
		count++
	}
	result = fmt.Sprintf("%d counts, %d keys mismatch\n", count, mismatch_count)
	switch {
	case count > redis_key_count:
		result = result + fmt.Sprintf("redis key amount is less than leveldb:%d vs %d", redis_key_count, count)
	case count < redis_key_count:
		result = result + fmt.Sprintf("redis key amount is larger than leveldb:%d vs %d", redis_key_count, count)
	default:
		result = result + fmt.Sprintf("%d key compared, %d mismatch", count, mismatch_count)
	}

	return
}

func fast_check(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	cli := context.redis
	db := context.db

	detail := false
	if len(args) > 0 && args[0] == "detail" {
		detail = true
	}

	var miss []string
	var mismatch []string
	if detail {
		miss = make([]string, 0)
		mismatch = make([]string, 0)
	}
	miss_count := 0
	match_count := 0
	mismatch_count := 0
	ret, err := cli.Exec("keys", "*")
	if err != nil {
		return
	}
	keys := ret.([]string)
	total := len(keys)

	var cur_version string // redis
	var bak_version []byte // leveldb
	for i, key := range keys {
		if ret, err = cli.Hget(key, "version"); err != nil {
			return
		}
		cur_version = ret.(string)
		index_key := indexKey(key)
		if bak_version, err = db.Get([]byte(index_key)); err != nil {
			return
		}
		if bak_version == nil {
			if miss != nil {
				miss = append(miss, key)
			}
			miss_count++
		} else if cur_version != string(bak_version) {
			if mismatch != nil {
				mismatch = append(mismatch, key)
			}
			mismatch_count++
		} else {
			match_count++
		}

		if i%1000 == 0 {
			Info("fast check progress:%d/%d\n", i, total)
		}
	}

	buf := bytes.NewBufferString("fast check results:\n")
	fmt.Fprintf(buf, "total: %d\n", total)
	fmt.Fprintf(buf, "miss: %d\n", miss_count)
	fmt.Fprintf(buf, "mismatch: %d\n", mismatch_count)
	fmt.Fprintf(buf, "match: %d\n", match_count)
	if detail {
		if mismatch_count > 0 {
			fmt.Fprintf(buf, "mismatch keys: %s\n", strings.Join(mismatch, ", "))
		}
		if miss_count > 0 {
			fmt.Fprintf(buf, "miss keys: %s\n", strings.Join(miss, ", "))
		}
	}
	result = buf.String()
	return
}

func dump(ud interface{}, args []string) (result string, err error) {
	if len(args) == 0 {
		err = errors.New("no key")
		return
	}

	key := args[0]
	context := ud.(*Context)
	db := context.db

	chunk, err := db.Get([]byte(key))
	if chunk == nil || err != nil {
		Error("fetch data failed:%v", err)
		return
	}

	Info("dump key:%s(%d)", key, len(chunk))
	var data map[string]string
	err = json.Unmarshal(chunk, &data)
	if err != nil {
		Error("unmarshal chunk failed:%v", err)
		return
	}

	buf := bytes.NewBufferString("content:\n")
	for key, val := range data {
		fmt.Fprintf(buf, "%v:\t%v\n", key, val)
	}
	result = buf.String()
	return
}

func restore_one(ud interface{}, args []string) (result string, err error) {
	if len(args) < 1 {
		err = errors.New("restore need one argument")
		return
	}

	key := args[0]
	context := ud.(*Context)
	db := context.db
	var chunk []byte
	if chunk, err = db.Get([]byte(key)); err != nil {
		Error("query key %s failed:%v", key, err)
		return
	}

	var leveldb_data map[string]string
	err = json.Unmarshal(chunk, &leveldb_data)
	cli := context.redis
	redis_data := make(map[string]string)
	err = cli.Hgetall(key, redis_data)
	if err != nil {
		Error("hgetall key %s failed:%v", key, err)
		return
	}

	if redis_data["version"] >= leveldb_data["version"] && len(redis_data) > 0 {
		result = fmt.Sprintf("skip key:%s version:%v == %v", key, redis_data["version"], leveldb_data["version"])
		return
	}
	leveldb_array := make([]interface{}, len(leveldb_data)*2+1)
	leveldb_array[0] = key
	i := 1
	for k, v := range leveldb_data {
		leveldb_array[i] = k
		leveldb_array[i+1] = v
		i = i + 2
	}
	_, err = cli.Exec("hmset", leveldb_array...)
	if err != nil {
		Error("hmset key %s failed:%v", key, err)
		return
	}
	result = fmt.Sprintf("set key:%s", key)
	return
}

func restore_all(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	count := 0
	restore_count := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		result, err = restore_one(ud, []string{string(it.Key())})
		if strings.HasPrefix(result, "set key") {
			restore_count++
		} else {
			Error(result)
		}
		count++
		if count%100 == 0 {
			Info("progress:%d, restore:%d", count, restore_count)
		}
	}
	result = fmt.Sprintf("restore key %d, total %d\n", restore_count, count)
	return
}

func keys(ud interface{}, args []string) (result string, err error) {
	start := 0
	count := 10
	if len(args) > 0 {
		if start, err = strconv.Atoi(args[0]); err != nil {
			Error("iter start error: %v", err)
			return
		}
	}

	if len(args) > 1 {
		if count, err = strconv.Atoi(args[1]); err != nil {
			Error("iter start error: %v", err)
			return
		}
	}

	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	defer it.Close()

	buf := bytes.NewBufferString("keys:\n")
	i := 0
	for it.Seek(INDEX_KEY_START); it.Valid() && bytes.Compare(it.Key(), INDEX_KEY_END) <= 0; it.Next() {
		if start <= i && i <= start+count {
			fmt.Fprintf(buf, "%s\n", string(it.Key()[INDEX_KEY_LEN:]))
		}
		i++
	}
	result = buf.String()
	return
}

func diff(ud interface{}, args []string) (result string, err error) {
	if len(args) == 0 {
		err = errors.New("no key")
		return
	}

	key := args[0]
	context := ud.(*Context)

	cli := context.redis
	db := context.db
	// query redis
	left := make(map[string]string)
	err = cli.Hgetall(key, left)
	if err != nil {
		return
	}

	chunk, err := db.Get([]byte(key))
	if chunk == nil || err != nil {
		Error("fetch data failed:%v", err)
		return
	}

	var right map[string]string
	err = json.Unmarshal(chunk, &right)
	if err != nil {
		Error("unmarshal chunk failed:%v", err)
		return
	}

	buf := bytes.NewBufferString("left:redis, right:leveldb\n")
	buf_len := buf.Len()
	for k, v1 := range left {
		if v2, ok := right[k]; ok {
			if v1 != v2 {
				fmt.Fprintf(buf, "%s < %s, %s\n", k, v1, v2)
			}
		} else {
			fmt.Fprintf(buf, "%s, only in left\n", k)
		}
	}

	for k, _ := range right {
		if _, ok := left[k]; !ok {
			fmt.Fprintf(buf, "%s, only in right\n", k)
		}
	}

	if buf_len == buf.Len() {
		fmt.Fprintf(buf, "perfect match\n")
	}

	result = buf.String()
	return
}

func (context *Context) Register(c *CmdService) {
	err := context.redis.Connect()
	if err != nil {
		Panic("register cmd service failed:%v", err)
	}

	Info("register command service")
	c.Register("help", context, help)
	c.Register("procs", context, procs)
	c.Register("info", context, info)
	c.Register("sync", context, sync_one)
	c.Register("sync_all", context, sync_all)
	c.Register("dump", context, dump)
	c.Register("count", context, count)
	c.Register("diff", context, diff)
	c.Register("shutdown", context, shutdown)
	c.Register("keys", context, keys)
	c.Register("check", context, check)
	c.Register("fast_check", context, fast_check)
	c.Register("restore_one", context, restore_one)
	c.Register("restore_all", context, restore_all)
}

func NewContext() *Context {
	context := new(Context)
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	context.redis = cli
	context.quit_chan = make(chan bool)
	return context
}
