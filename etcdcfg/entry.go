package etcdcfg

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/walleframe/walle/services/configcentra"
)

var config = struct {
	addrs []string
	user  string
	pass  string
}{
	addrs: []string{"127.0.0.1:2379"},
}

func InitFlag() {
	// command line flag
	pflag.StringSliceVarP(&config.addrs, "etcd-addrs", "a", config.addrs, "etcd addrs")
	pflag.StringVarP(&config.user, "user", "u", config.user, "etcd user")
	pflag.StringVarP(&config.pass, "pass", "p", config.pass, "etcd pass")
}

// UseEtcdConfig read config from etcd.
func UseEtcdConfig() {
	// new backend
	svc := NewEtcdConfigBackend("config/", time.Second, false)
	// set backend
	configcentra.ConfigCentraBackend = svc
	// init command flag
	InitFlag()
}
