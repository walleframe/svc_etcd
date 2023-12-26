package etcdplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/walleframe/walle/app"
	"github.com/walleframe/walle/kvstore"
	"github.com/walleframe/walle/xlsxmgr"

	"github.com/walleframe/svc_etcd/kvstore/etcdv3"
)

// XlsxEtcdConfig config xlsx etcd
//
//go:generate gogen cfggen -n XlsxEtcdConfig -o config.go
func walleXlsxEtcdConfig() interface{} {
	return map[string]interface{}{
		// etcd endpoints
		"Endpoints": []string{"127.0.0.1:2379"},
		// etcd user name
		"UserName": "",
		// etcd password
		"Password": "",
		// dial etcd timeout config
		"DialTimeout": time.Duration(time.Second * 5),
		// etcd path
		"ConfigPath": "xlsxcfg/",
		// etcd file ext
		"FileExt": ".json",
	}
}

var (
	cfg        = NewXlsxEtcdConfig("xlsxcfg.etcd")
	XlsxPlugin = &EtcdConfigLoad{}
)

type EtcdConfigLoad struct {
	stoper  app.Stoper
	kvstore kvstore.Store
	mgr     *xlsxmgr.XlsxConfig
}

var _ xlsxmgr.XlsxLoadPlugin = (*EtcdConfigLoad)(nil)

func (r *EtcdConfigLoad) Name() string {
	return "etcd"
}

func (r *EtcdConfigLoad) Start(ctx context.Context, mgr *xlsxmgr.XlsxConfig, s app.Stoper) error {
	r.mgr = mgr
	r.stoper = s

	log := mgr.Logger().New("EtcdPlugin.Start")

	kvstore, err := etcdv3.New(
		etcdv3.WithEndpoints(cfg.Endpoints...),
		etcdv3.WithUserName(cfg.UserName),
		etcdv3.WithPassword(cfg.Password),
		etcdv3.WithLogger(r.mgr.Logger()),
		etcdv3.WithContext(ctx),
		etcdv3.WithDialTimeout(cfg.DialTimeout),
	)
	if err != nil {
		return err
	}

	r.kvstore = kvstore

	// 读取远端配置
	kvs, err := kvstore.List(ctx, cfg.ConfigPath)
	if err != nil {
		log.Error("list config failed",
			zap.String("path", cfg.ConfigPath),
			zap.Error(err),
		)
		return err
	}

	// 更新配置
	err = r.updateConfig(kvs)
	if err != nil {
		log.Error("first load config failed",
			zap.String("path", cfg.ConfigPath),
			zap.Error(err),
		)
		return err
	}

	// 监听配置变更
	ch, err := kvstore.Watch(ctx, filepath.Join(cfg.ConfigPath, "update_notify"), r.stoper.GetStopChan())
	if err != nil {
		log.Error("watch config failed",
			zap.String("path", cfg.ConfigPath),
			zap.Error(err),
		)
		return err
	}
	go r.watchConfigUpdate(ch, mgr)
	return nil
}

// stop
func (r *EtcdConfigLoad) Stop(ctx context.Context) {
	r.kvstore.Close(ctx)
}

// UnmarshalXlsxData unmarshal xlsx data to object
func (r *EtcdConfigLoad) UnmarshalXlsxData(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (r *EtcdConfigLoad) watchConfigUpdate(ch <-chan *kvstore.KVPair, mgr *xlsxmgr.XlsxConfig) {
	log := r.mgr.Logger().New("EtcdPlugin.watchConfigUpdate")
	for {
		select {
		case kv := <-ch:
			cfgTime := string(kv.Value)
			// log.Println("config update time:", cfgTime)
			log.Info("update remote config", zap.String("updateConfigTime", cfgTime))
			kvs, err := r.kvstore.List(context.Background(), cfg.ConfigPath)
			if err != nil {
				log.Error("update config,load list failed",
					zap.String("path", cfg.ConfigPath),
					zap.Error(err),
				)
				continue
			}
			r.updateConfig(kvs)

		case <-r.stoper.GetStopChan():
			return
		}
	}
}

func (r *EtcdConfigLoad) updateConfig(kvs []*kvstore.KVPair) (err error) {
	log := r.mgr.Logger().New("EtcdPlugin.updateConfig")
	// 转换map
	data := make(map[string][]byte, len(kvs))
	for _, kv := range kvs {
		fname := strings.TrimPrefix(kv.Key, cfg.ConfigPath)
		fname = strings.TrimPrefix(fname, "/")
		data[fname] = kv.Value
	}

	// 首次根据注册依赖加载文件配置
	r.mgr.Range(func(value *xlsxmgr.ConfigItem) bool {
		fname := value.GetFileName(cfg.FileExt)
		if fd, ok := data[fname]; ok {
			// log.Println("load file", fname)
			err = value.LoadConfigFromData(fd)
			if err != nil {
				log.Error("load config failed",
					zap.String("path", cfg.ConfigPath),
					zap.String("fname", fname),
					zap.Error(err),
				)
				return false
			}
			//log.Info("xlsxcfg.Remote update remote config success", zap.String("file", fname))
			return true
		}
		err = fmt.Errorf("not found %s config", fname)
		log.Error("find config failed",
			zap.String("path", cfg.ConfigPath),
			zap.String("fname", fname),
			zap.Error(err),
		)
		return false
	})
	if err != nil {
		return err
	}
	r.mgr.Range(func(value *xlsxmgr.ConfigItem) bool {
		value.BuildMixData()
		return true
	})
	return
}
