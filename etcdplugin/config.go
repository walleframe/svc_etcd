// Code generated by "gogen cfggen"; DO NOT EDIT.
// Exec: gogen cfggen -n XlsxEtcdConfig -o config.go Version: 0.0.1
package etcdplugin

import (
	"time"

	"github.com/walleframe/walle/services/configcentra"
)

var _ = walleXlsxEtcdConfig()

// XlsxEtcdConfig config generate by gogen cfggen.
type XlsxEtcdConfig struct {
	// etcd endpoints
	Endpoints []string `json:"endpoints,omitempty"`
	// etcd user name
	UserName string `json:"username,omitempty"`
	// etcd password
	Password string `json:"password,omitempty"`
	// dial etcd timeout config
	DialTimeout time.Duration `json:"dialtimeout,omitempty"`
	// etcd path
	ConfigPath string `json:"configpath,omitempty"`
	// etcd file ext
	FileExt string `json:"fileext,omitempty"`
	// config prefix string
	prefix string
	// update ntf funcs
	ntfFuncs []func(*XlsxEtcdConfig)
}

var _ configcentra.ConfigValue = (*XlsxEtcdConfig)(nil)

func NewXlsxEtcdConfig(prefix string) *XlsxEtcdConfig {
	if prefix == "" {
		panic("config prefix invalid")
	}
	// new default config value
	cfg := NewDefaultXlsxEtcdConfig(prefix)
	// register value to config centra
	configcentra.RegisterConfig(cfg)
	return cfg
}

func NewDefaultXlsxEtcdConfig(prefix string) *XlsxEtcdConfig {
	cfg := &XlsxEtcdConfig{
		Endpoints:   []string{"127.0.0.1:2379"},
		UserName:    "",
		Password:    "",
		DialTimeout: time.Second * 5,
		ConfigPath:  "xlsxcfg/",
		FileExt:     ".json",
		prefix:      prefix,
	}
	return cfg
}

// add notify func
func (cfg *XlsxEtcdConfig) AddNotifyFunc(f func(*XlsxEtcdConfig)) {
	cfg.ntfFuncs = append(cfg.ntfFuncs, f)
}

// impl configcentra.ConfigValue
func (cfg *XlsxEtcdConfig) SetDefaultValue(cc configcentra.ConfigCentra) {
	if cc.UseObject() {
		cc.SetObject(cfg.prefix, "XlsxEtcdConfig config xlsx etcd", cfg)
		return
	}
	cc.SetDefault(cfg.prefix+".endpoints", "etcd endpoints", cfg.Endpoints)
	cc.SetDefault(cfg.prefix+".username", "etcd user name", cfg.UserName)
	cc.SetDefault(cfg.prefix+".password", "etcd password", cfg.Password)
	cc.SetDefault(cfg.prefix+".dialtimeout", "dial etcd timeout config", cfg.DialTimeout)
	cc.SetDefault(cfg.prefix+".configpath", "etcd path", cfg.ConfigPath)
	cc.SetDefault(cfg.prefix+".fileext", "etcd file ext", cfg.FileExt)
}

// impl configcentra.ConfigValue
func (cfg *XlsxEtcdConfig) RefreshValue(cc configcentra.ConfigCentra) error {
	if cc.UseObject() {
		return cc.GetObject(cfg.prefix, cfg)
	}
	{
		v, err := cc.GetStringSlice(cfg.prefix + ".endpoints")
		if err != nil {
			return err
		}
		cfg.Endpoints = ([]string)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".username")
		if err != nil {
			return err
		}
		cfg.UserName = (string)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".password")
		if err != nil {
			return err
		}
		cfg.Password = (string)(v)
	}
	{
		v, err := cc.GetDuration(cfg.prefix + ".dialtimeout")
		if err != nil {
			return err
		}
		cfg.DialTimeout = (time.Duration)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".configpath")
		if err != nil {
			return err
		}
		cfg.ConfigPath = (string)(v)
	}
	{
		v, err := cc.GetString(cfg.prefix + ".fileext")
		if err != nil {
			return err
		}
		cfg.FileExt = (string)(v)
	}
	// notify update
	for _, ntf := range cfg.ntfFuncs {
		ntf(cfg)
	}
	return nil
}