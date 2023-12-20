package etcdcfg

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/walleframe/svc_etcd/kvstore/etcdv3"
	"github.com/walleframe/walle/app"
	"github.com/walleframe/walle/kvstore"
	"github.com/walleframe/walle/services/configcentra"
	"github.com/walleframe/walle/util"
	"github.com/walleframe/walle/zaplog"
	"go.uber.org/multierr"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type ConfigItem struct {
	Value configcentra.ConfigValue
	Ntfs  []configcentra.ConfigUpdateNotify
}

type EtcdConfigBackend struct {
	store      kvstore.Store
	values     []ConfigItem
	ntfs       []configcentra.ConfigUpdateNotify
	prefix     string
	timeout    time.Duration
	setDefault bool
}

var _ configcentra.ConfigCentra = (*EtcdConfigBackend)(nil)

func NewEtcdConfigBackend(prefix string, timeout time.Duration, setDefault bool) *EtcdConfigBackend {
	return &EtcdConfigBackend{
		store:      nil,
		prefix:     prefix,
		timeout:    timeout,
		setDefault: setDefault,
	}
}

func (svc *EtcdConfigBackend) Init(s app.Stoper) (err error) {
	svc.store, err = etcdv3.New(
		etcdv3.WithEndpoints(config.addrs...),
		etcdv3.WithUserName(config.user),
		etcdv3.WithPassword(config.pass),
		etcdv3.WithDialTimeout(svc.timeout),
		etcdv3.WithLogger(zaplog.GetFrameLogger().Named("EtcdConfigBackend")),
		etcdv3.WithNamespace(svc.prefix),
	)
	if err != nil {
		return err
	}

	// set default value
	for _, v := range svc.values {
		v.Value.SetDefaultValue(svc)
	}

	for _, v := range svc.values {
		err = multierr.Append(err, v.Value.RefreshValue(svc))
	}

	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-s.GetStopChan():
			cancel()
		}
	}()

	ch, err := svc.store.WatchTree(ctx, svc.prefix, s.GetStopChan())
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case _, ok := <-ch:
				if ok {
					return
				}
				svc.onUpdateConfig()
			case <-s.GetStopChan():
				return
			}
		}
	}()

	return
}
func (svc *EtcdConfigBackend) Start(s app.Stoper) error {
	return nil
}
func (svc *EtcdConfigBackend) Stop() {
	return
}
func (svc *EtcdConfigBackend) Finish() {
	return
}

func (svc *EtcdConfigBackend) onUpdateConfig() {
	for _, vc := range svc.values {
		if err := vc.Value.RefreshValue(svc); err != nil {
			log.Println("update config failed", err)
			continue
		}
		for _, ntf := range vc.Ntfs {
			ntf(svc)
		}
	}
	for _, ntf := range svc.ntfs {
		ntf(svc)
	}
	return
}

// register custom config value
func (svc *EtcdConfigBackend) RegisterConfig(v configcentra.ConfigValue, ntf []configcentra.ConfigUpdateNotify) {
	svc.values = append(svc.values, ConfigItem{
		Value: v,
		Ntfs:  ntf,
	})
}

// watch config update
func (svc *EtcdConfigBackend) WatchConfigUpdate(ntf []configcentra.ConfigUpdateNotify) {
	svc.ntfs = append(svc.ntfs, ntf...)
}

// object support
func (svc *EtcdConfigBackend) UseObject() bool {
	return true
}

func (svc *EtcdConfigBackend) SetObject(key string, doc string, obj interface{}) {
	if !svc.setDefault {
		return
	}
	//
	key = svc.key(key)
	data, err := svc.get(key)
	if err != nil {
		log.Println("set default value failed", err)
		return
	}
	if data != nil {
		return
	}
	data, err = json.Marshal(obj)
	if err != nil {
		log.Println("marshal object failed", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), svc.timeout)
	defer cancel()
	err = svc.store.Put(ctx, key, data)
	if err != nil {
		log.Println("set object default value failed", err)
		return
	}
}

func (svc *EtcdConfigBackend) GetObject(key string, obj interface{}) (err error) {
	data, err := svc.get(key)
	if err != nil {
		return err
	}

	if data == nil {
		return
	}
	return json.Unmarshal(data, obj)
}

func (svc *EtcdConfigBackend) SetDefault(key string, doc string, value interface{}) {
	if !svc.setDefault {
		return
	}
	//
	key = svc.key(key)
	data, err := svc.get(key)
	if err != nil {
		log.Println("set default value failed", err)
		return
	}
	if data != nil {
		return
	}
	data = []byte(fmt.Sprint(value))
	ctx, cancel := context.WithTimeout(context.Background(), svc.timeout)
	defer cancel()
	err = svc.store.Put(ctx, key, data)
	if err != nil {
		log.Println("set object default value failed", err)
		return
	}
}

func (svc *EtcdConfigBackend) GetString(key string) (_ string, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	return string(data), nil
}
func (svc *EtcdConfigBackend) GetBool(key string) (_ bool, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	return strconv.ParseBool(util.BytesToString(data))
}
func (svc *EtcdConfigBackend) GetInt(key string) (_ int, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	return strconv.Atoi(util.BytesToString(data))
}
func (svc *EtcdConfigBackend) GetInt32(key string) (_ int32, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseInt(util.BytesToString(data), 10, 64)
	if err != nil {
		return
	}

	return int32(v), nil

}
func (svc *EtcdConfigBackend) GetInt64(key string) (_ int64, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseInt(util.BytesToString(data), 10, 64)
	if err != nil {
		return
	}

	return int64(v), nil
}
func (svc *EtcdConfigBackend) GetUint(key string) (_ uint, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseUint(util.BytesToString(data), 10, 64)
	if err != nil {
		return
	}

	return uint(v), nil
}
func (svc *EtcdConfigBackend) GetUint16(key string) (_ uint16, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseUint(util.BytesToString(data), 10, 16)
	if err != nil {
		return
	}

	return uint16(v), nil
}
func (svc *EtcdConfigBackend) GetUint32(key string) (_ uint32, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseUint(util.BytesToString(data), 10, 32)
	if err != nil {
		return
	}

	return uint32(v), nil
}
func (svc *EtcdConfigBackend) GetUint64(key string) (_ uint64, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseUint(util.BytesToString(data), 10, 64)
	if err != nil {
		return
	}

	return uint64(v), nil
}
func (svc *EtcdConfigBackend) GetFloat64(key string) (_ float64, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := strconv.ParseFloat(util.BytesToString(data), 64)
	if err != nil {
		return
	}

	return float64(v), nil
}
func (svc *EtcdConfigBackend) GetTime(key string) (_ time.Time, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := time.Parse(util.BytesToString(data), time.RFC3339)
	if err != nil {
		return
	}

	return v, nil
}
func (svc *EtcdConfigBackend) GetDuration(key string) (_ time.Duration, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	v, err := time.ParseDuration(util.BytesToString(data))
	if err != nil {
		return
	}

	return v, nil
}
func (svc *EtcdConfigBackend) GetIntSlice(key string) (vals []int, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	array := strings.Split(util.BytesToString(data), ",")
	for _, num := range array {
		v, err := strconv.Atoi(num)
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	return
}
func (svc *EtcdConfigBackend) GetStringSlice(key string) (_ []string, err error) {
	data, err := svc.get(key)
	if err != nil {
		return
	}

	if data == nil {
		return
	}
	return strings.Split(util.BytesToString(data), ","), nil
}

func (svc *EtcdConfigBackend) key(in string) string {
	return filepath.ToSlash(strings.Replace(in, ".", "/", -1))
}

func (svc *EtcdConfigBackend) get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	kv, err := svc.store.Get(ctx, svc.key(key))
	if err != nil && err != kvstore.ErrKeyNotFound {
		return nil, err
	}
	if kv == nil {
		return nil, nil
	}
	return kv.Value, nil
}
