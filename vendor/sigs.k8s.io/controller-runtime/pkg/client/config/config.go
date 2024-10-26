/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"flag"
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	logf "sigs.k8s.io/controller-runtime/pkg/internal/log"
)

// KubeconfigFlagName is the name of the kubeconfig flag
const KubeconfigFlagName = "kubeconfig"

var (
	kubeconfig string
	log        = logf.RuntimeLog.WithName("client").WithName("config")
)

// init registers the "kubeconfig" flag to the default command line FlagSet.
// TODO: This should be removed, as it potentially leads to redefined flag errors for users, if they already
// have registered the "kubeconfig" flag to the command line FlagSet in other parts of their code.
func init() {
	RegisterFlags(flag.CommandLine)
}

// RegisterFlags registers flag variables to the given FlagSet if not already registered.
// It uses the default command line FlagSet, if none is provided. Currently, it only registers the kubeconfig flag.
func RegisterFlags(fs *flag.FlagSet) {
	if fs == nil {
		fs = flag.CommandLine
	}
	if f := fs.Lookup(KubeconfigFlagName); f != nil {
		kubeconfig = f.Value.String()
	} else {
		fs.StringVar(&kubeconfig, KubeconfigFlagName, "", "Paths to a kubeconfig. Only required if out-of-cluster.")
	}
}

// GetConfig creates a *rest.Config for talking to a Kubernetes API server.
// If --kubeconfig is set, will use the kubeconfig file at that location.  Otherwise will assume running
// in cluster and use the cluster provided kubeconfig.
//
// It also applies saner defaults for QPS and burst based on the Kubernetes
// controller manager defaults (20 QPS, 30 burst)
//
// Config precedence:
//
// * --kubeconfig flag pointing at a file
//
// * KUBECONFIG environment variable pointing at a file
//
// * In-cluster config if running in cluster
//
// * $HOME/.kube/config if exists.
func GetConfig() (*rest.Config, error) {
	return GetConfigWithContext("")
}

// GetConfigWithContext creates a *rest.Config for talking to a Kubernetes API server with a specific context.
// If --kubeconfig is set, will use the kubeconfig file at that location.  Otherwise will assume running
// in cluster and use the cluster provided kubeconfig.
//
// It also applies saner defaults for QPS and burst based on the Kubernetes
// controller manager defaults (20 QPS, 30 burst)
//
// Config precedence(优先级):
//
// * --kubeconfig flag pointing at a file
//
// * KUBECONFIG environment variable pointing at a file
//
// * In-cluster config if running in cluster
//
// * $HOME/.kube/config if exists.
/*
创建一个*rest.Config对象，用于配置与Kubernetes API服务器通信所需的参数。
为与特定上下文的Kubernetes API服务器通信创建一个配置
这个函数考虑了不同的配置来源，并设置了一些默认值，特别是针对QPS(每秒查询率 20)和Burst(突发流量 30)的值
如果设置了 --kubeconfig 参数，将使用该位置的kubeconfig文件，否则将假设在集群中运行，
并使用集群提供的kubeconfig
*/
func GetConfigWithContext(context string) (*rest.Config, error) {
	cfg, err := loadConfig(context)
	if err != nil {
		return nil, err
	}
	if cfg.QPS == 0.0 {
		cfg.QPS = 20.0
	}
	if cfg.Burst == 0 {
		cfg.Burst = 30
	}
	return cfg, nil
}

// loadInClusterConfig is a function used to load the in-cluster
// Kubernetes client config. This variable makes is possible to
// test the precedence of loading the config.
var loadInClusterConfig = rest.InClusterConfig

// loadConfig loads a REST Config as per the rules specified in GetConfig.
func loadConfig(context string) (config *rest.Config, configErr error) {
	// If a flag is specified with the config location, use that
	// 如果通过 --kubeconfig 指定了配置文件的位置
	if len(kubeconfig) > 0 {
		return loadConfigWithContext("", &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}, context)
	}

	// If the recommended kubeconfig env variable is not specified,
	// try the in-cluster config.
	// 环境变量 KUBECONFIG 未指定（这是 Kubernetes 推荐的配置路径环境变量）
	// 则尝试使用集群内的配置
	kubeconfigPath := os.Getenv(clientcmd.RecommendedConfigPathEnvVar)
	if len(kubeconfigPath) == 0 {
		c, err := loadInClusterConfig()
		if err == nil {
			return c, nil
		}

		defer func() {
			if configErr != nil {
				// 确保在函数返回之前，如果 configErr 不为 nil ，那么会记录一条错误日志。
				log.Error(err, "unable to load in-cluster config")
			}
		}()
	}

	// If the recommended kubeconfig env variable is set, or there
	// is no in-cluster config, try the default recommended locations.
	//
	// NOTE: For default config file locations, upstream only checks
	// $HOME for the user's home directory, but we can also try
	// os/user.HomeDir when $HOME is unset.
	//
	// TODO(jlanford): could this be done upstream?
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if _, ok := os.LookupEnv("HOME"); !ok {
		// HOME 环境变量未设置
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("could not get current user: %w", err)
		}
		// 设置 Kubernetes 配置文件的搜索路径
		// Precedence 按优先级顺序搜索配置文件的路径的列表
		// 将当前用户的主目录下的 Kubernetes 推荐配置文件路径追加到 loadingRules.Precedence 列表中
		//将用户的主目录(u.HomeDir)、推荐的子目录(.kube)和推荐的文件名(config)连接起来，形成一个完整的配置文件路径。
		loadingRules.Precedence = append(loadingRules.Precedence, filepath.Join(u.HomeDir, clientcmd.RecommendedHomeDir, clientcmd.RecommendedFileName))
	}

	return loadConfigWithContext("", loadingRules, context)
}

/*
函数使用 clientcmd.NewNonInteractiveDeferredLoadingClientConfig 创建一个新的客户端配置对象，
这个对象根据提供的加载器和覆盖配置来加载和获取客户端配置。

函数将 API 服务器的 URL 和当前上下文传递给 ConfigOverrides 对象，
然后调用 ClientConfig 方法来获取实际的 *rest.Config 对象。
*/
func loadConfigWithContext(apiServerURL string, loader clientcmd.ClientConfigLoader, context string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loader,
		&clientcmd.ConfigOverrides{
			ClusterInfo: clientcmdapi.Cluster{
				Server: apiServerURL,
			},
			CurrentContext: context,
		}).ClientConfig()
}

// GetConfigOrDie creates a *rest.Config for talking to a Kubernetes apiserver.
// If --kubeconfig is set, will use the kubeconfig file at that location.  Otherwise will assume running
// in cluster and use the cluster provided kubeconfig.
//
// Will log an error and exit if there is an error creating the rest.Config.
func GetConfigOrDie() *rest.Config {
	config, err := GetConfig()
	if err != nil {
		log.Error(err, "unable to get kubeconfig")
		os.Exit(1)
	}
	return config
}
