/*
Copyright 2022 The Fluid Authors.

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

package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/csi"
	"github.com/fluid-cloudnative/fluid/pkg/csi/config"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	utilfeature "github.com/fluid-cloudnative/fluid/pkg/utils/feature"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	endpoint              string
	nodeID                string
	metricsAddr           string
	pprofAddr             string
	pruneFs               []string
	prunePath             string
	kubeletKubeConfigPath string
)

var scheme = runtime.NewScheme()

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start fluid driver on node",
	Run: func(cmd *cobra.Command, args []string) {
		handle()
	},
}

func init() {
	// Register k8s-native resources and Fluid CRDs
	// 将 k8s 官方提供的 clientgoscheme 包中的 Scheme 对象添加到自定义的 scheme 对象中,
	// 并将 datav1alpha1 包中的 CRD 添加到 scheme 对象中。
	// clientgoscheme 包中包含了 k8s 核心资源，如 Pod, Service, Namespace 等的序列化和反序列化逻辑
	_ = clientgoscheme.AddToScheme(scheme)
	// datav1aplha1 包中包含 CRD 如 Dataset, AlluxioRuntime 等，以及这些 CRD 的序列化和反序列化逻辑
	_ = datav1alpha1.AddToScheme(scheme)

	if err := flag.Set("logtostderr", "true"); err != nil {
		fmt.Printf("Failed to flag.set due to %v", err)
		os.Exit(1)
	}

	startCmd.Flags().StringVarP(&nodeID, "nodeid", "", "", "node id")
	if err := startCmd.MarkFlagRequired("nodeid"); err != nil {
		ErrorAndExit(err)
	}

	startCmd.Flags().StringVarP(&endpoint, "endpoint", "", "", "CSI endpoint")
	if err := startCmd.MarkFlagRequired("endpoint"); err != nil {
		ErrorAndExit(err)
	}

	startCmd.Flags().StringSliceVarP(&pruneFs, "prune-fs", "", []string{"fuse.alluxio-fuse", "fuse.jindofs-fuse", "fuse.juicefs", "fuse.goosefs-fuse", "ossfs"}, "Prune fs to add in /etc/updatedb.conf, separated by comma")
	startCmd.Flags().StringVarP(&prunePath, "prune-path", "", "/runtime-mnt", "Prune path to add in /etc/updatedb.conf")
	startCmd.Flags().StringVarP(&metricsAddr, "metrics-addr", "", ":8080", "The address the metrics endpoint binds to.")
	startCmd.Flags().StringVarP(&pprofAddr, "pprof-addr", "", "", "The address for pprof to use while exporting profiling results")
	startCmd.Flags().StringVarP(&kubeletKubeConfigPath, "kubelet-kube-config", "", "/etc/kubernetes/kubelet.conf", "The file path to kubelet kube config")
	utilfeature.DefaultMutableFeatureGate.AddFlag(startCmd.Flags())
	startCmd.Flags().AddGoFlagSet(flag.CommandLine)
}

func ErrorAndExit(err error) {
	fmt.Fprintf(os.Stderr, "%s", err.Error())
	os.Exit(1)
}

func handle() {
	// startReaper()
	fluid.LogVersion()

	if pprofAddr != "" {
		newPprofServer(pprofAddr)
	}

	// the default webhook server port is 9443, no need to set
	// 创建控制器管理器实例
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
	})

	if err != nil {
		panic(fmt.Sprintf("csi: unable to create controller manager due to error %v", err))
	}

	// 存储运行时的配置信息
	runningContext := config.RunningContext{
		Config: config.Config{
			NodeId:            nodeID,
			Endpoint:          endpoint,
			PruneFs:           pruneFs,
			PrunePath:         prunePath,
			KubeletConfigPath: kubeletKubeConfigPath,
		},
		// 管理卷的锁定状态
		VolumeLocks: utils.NewVolumeLocks(),
	}
	if err = csi.SetupWithManager(mgr, runningContext); err != nil {
		panic(fmt.Sprintf("unable to set up manager due to error %v", err))
	}

	ctx := ctrl.SetupSignalHandler()
	if err = mgr.Start(ctx); err != nil {
		panic(fmt.Sprintf("unable to start controller recover due to error %v", err))
	}
}

// 在指定地址上提供 Go 语言的性能分析工具。
func newPprofServer(pprofAddr string) {
	glog.Infof("Enabling pprof with address %s", pprofAddr)
	// mux 用于处理 HTTP 请求。
	mux := http.NewServeMux()
	// 将 /debug/pprof/ 路径的请求映射到 pprof.Index 函数。用于显示 pprof 索引界面。
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	// 显示 pprof 命令行参数。
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	// 用于进行 CPU 性能分析。
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	// 用于解析程序的符号表。
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	// 用于程序的跟踪分析。
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// 创建一个新的 http.Server 实例，设置监听地址为 pprofAddr，处理请求的处理器为 mux。
	pprofServer := http.Server{
		Addr:    pprofAddr,
		Handler: mux,
	}

	glog.Infof("Starting pprof HTTP server at %s", pprofServer.Addr)

	// 用于处理服务器的生命周期管理
	go func() {
		// 用于监听上下文的取消信号
		go func() {
			ctx := context.Background()
			// 阻塞等待，直到上下文被取消
			<-ctx.Done()

			ctx, cancelFunc := context.WithTimeout(context.Background(), 60*time.Minute)
			defer cancelFunc()

			if err := pprofServer.Shutdown(ctx); err != nil {
				glog.Error(err, "Failed to shutdown debug HTTP server")
			}
		}()

		if err := pprofServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			glog.Error(err, "Failed to start debug HTTP server")
			panic(err)
		}
	}()
}
