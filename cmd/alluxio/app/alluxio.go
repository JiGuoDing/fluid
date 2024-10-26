/*
Copyright 2021 The Fluid Authors.

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
	"os"
	"time"

	// +kubebuilder:scaffold:imports

	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/controllers"
	alluxioctl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/alluxio"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/alluxio"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base/portallocator"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/spf13/cobra"
	zapOpt "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/net"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	// Use compiler to check if the struct implements all the interface
	// base.Implement类型的 _ 将被赋值为一个类型为 *alluxio.AlluxioEngine的空指针
	_ base.Implement = (*alluxio.AlluxioEngine)(nil)

	metricsAddr             string
	enableLeaderElection    bool
	leaderElectionNamespace string
	development             bool
	portRange               string
	maxConcurrentReconciles int
	pprofAddr               string
	portAllocatePolicy      string

	kubeClientQPS   float32
	kubeClientBurst int
)

// configuration for controllers' rate limiter
var (
	controllerWorkqueueDefaultSyncBackoffStr string
	controllerWorkqueueMaxSyncBackoffStr     string
	controllerWorkqueueQPS                   int
	controllerWorkqueueBurst                 int
)

var alluxioCmd = &cobra.Command{
	Use:   "start",
	Short: "start alluxioruntime-controller in Kubernetes",
	Run: func(cmd *cobra.Command, args []string) {
		handle()
	},
}

// 将 Kubernetes 客户端的默认 Scheme 和 datav1alpha1 API 类型注册到 scheme 中。
// 即将Kubernetes的内置类型和fluid定义的自定义资源定义（CRD）类型注册到scheme中。
func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = datav1alpha1.AddToScheme(scheme)
	// 对 alluxioCmd 命令注册和配置一系列的标志（flags），通过命令行参数提供对一些控制器配置项的自定义控制
	// cobra.Command.Flags().TypeVarP(p *type, name string, shorthand string, value type, usage string)
	// StringVarP与StringVar的区别在于是否支持标志的缩写

	// 指定用于绑定 metric 端点的地址，默认为 :8080，通常用于 Prometheus 等监控系统。
	alluxioCmd.Flags().StringVarP(&metricsAddr, "metrics-addr", "", ":8080", "The address the metric endpoint binds to.")
	// 启用或禁用领导选举机制，默认禁用。启用该选项可确保只有一个控制器实例处于活动状态，用于高可用的场景。
	alluxioCmd.Flags().BoolVarP(&enableLeaderElection, "enable-leader-election", "", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	// 指定领导选举资源将在哪个命名空间中创建，默认在 fluid-system 命名空间中。
	alluxioCmd.Flags().StringVarP(&leaderElectionNamespace, "leader-election-namespace", "", "fluid-system", "The namespace in which the leader election resource will be created.")
	// 是否启用开发模式，默认启用，通常用于调试和开发环境。
	alluxioCmd.Flags().BoolVarP(&development, "development", "", true, "Enable development mode for fluid controller.")
	// 指定 Alluxio 使用的节点端口范围，默认为 20000-25000。
	alluxioCmd.Flags().StringVar(&portRange, "runtime-node-port-range", "20000-25000", "Set available port range for Alluxio")
	// 指定 AlluxioRuntime 控制器的最大并发 worker 数量，默认为 3，控制同时执行的任务数。
	alluxioCmd.Flags().IntVar(&maxConcurrentReconciles, "runtime-workers", 3, "Set max concurrent workers for AlluxioRuntime controller")
	// 用于指定 pprof 的地址，pprof 是 Go 提供的性能分析工具，用于导出性能数据。
	alluxioCmd.Flags().StringVarP(&pprofAddr, "pprof-addr", "", "", "The address for pprof to use while exporting profiling results")
	// 设置端口分配策略，默认为 random，可选值有 bitmap 和 random。该选项控制端口分配的方式。
	alluxioCmd.Flags().StringVar(&portAllocatePolicy, "port-allocate-policy", "random", "Set port allocating policy, available choice is bitmap or random(default random).")
	// 与 Kubernetes API server 通信时的 QPS(Qureies Per Second) 限制，默认值为 20。
	// 控制器与 Kubernetes API 服务器每秒可以发送的最大请求数
	// 默认值是 20，意味着控制器每秒最多发送 20 个请求到 API 服务器。
	alluxioCmd.Flags().Float32VarP(&kubeClientQPS, "kube-api-qps", "", 20, "QPS to use while talking with kubernetes apiserver.") // 20 is the default qps in controller-runtime
	// 与 Kubernetes API server 通信时的突发流量限制，默认值为 30。
	// 突发情况下，控制器能瞬时发送的最大请求数，即使瞬间的请求数超过 kube-api-qps 的限制，也允许发送一定量的请求。
	// 默认值是 30，意味着控制器在短时间内可以发送最多 30 个请求，而不受 kube-api-qps 的限制。
	alluxioCmd.Flags().IntVarP(&kubeClientBurst, "kube-api-burst", "", 30, "Burst to use while talking with kubernetes apiserver.") // 30 is the default burst in controller-runtime
	// 设置控制器工作队列失败重试的基础回退时间，默认值为 5ms。
	alluxioCmd.Flags().StringVar(&controllerWorkqueueDefaultSyncBackoffStr, "workqueue-default-sync-backoff", "5ms", "base backoff period for failed reconciliation in controller's workqueue")
	// 设置控制器工作队列失败重试的最大回退时间，默认值为 1000s。
	alluxioCmd.Flags().StringVar(&controllerWorkqueueMaxSyncBackoffStr, "workqueue-max-sync-backoff", "1000s", "max backoff period for failed reconciliation in controller's workqueue")
	// 设置控制器工作队列的 QPS(Qureies Per Second) 限制，默认为 10。
	alluxioCmd.Flags().IntVar(&controllerWorkqueueQPS, "workqueue-qps", 10, "qps limit value for controller's workqueue")
	// 设置控制器工作队列的突发流量限制，默认为 100。
	alluxioCmd.Flags().IntVar(&controllerWorkqueueBurst, "workqueue-burst", 100, "burst limit value for controller's workqueue")
}

func handle() {
	fluid.LogVersion()

	ctrl.SetLogger(zap.New(func(o *zap.Options) {
		o.Development = development
	}, func(o *zap.Options) {
		o.ZapOpts = append(o.ZapOpts, zapOpt.AddCaller())
	}, func(o *zap.Options) {
		if !development {
			encCfg := zapOpt.NewProductionEncoderConfig()
			encCfg.EncodeLevel = zapcore.CapitalLevelEncoder
			encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
			o.Encoder = zapcore.NewConsoleEncoder(encCfg)
		}
	}))

	utils.NewPprofServer(setupLog, pprofAddr, development)

	// the default webhook server port is 9443, no need to set
	mgr, err := ctrl.NewManager(controllers.GetConfigOrDieWithQPSAndBurst(kubeClientQPS, kubeClientBurst), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		LeaderElection:          enableLeaderElection,
		LeaderElectionNamespace: leaderElectionNamespace,
		LeaderElectionID:        "alluxio.data.fluid.io",
		NewClient:               controllers.NewFluidControllerClient,
	})
	if err != nil {
		setupLog.Error(err, "unable to start alluxioruntime manager")
		os.Exit(1)
	}

	defaultSyncBackoff, err := time.ParseDuration(controllerWorkqueueDefaultSyncBackoffStr)
	if err != nil {
		setupLog.Error(err, "workqueue-default-sync-backoff is not a valid duration, please use string like \"100ms\", \"5s\", \"3m\", ...")
		os.Exit(1)
	}

	maxSyncBackoff, err := time.ParseDuration(controllerWorkqueueMaxSyncBackoffStr)
	if err != nil {
		setupLog.Error(err, "workqueue-max-sync-backoff is not a valid duration, please use string like \"100ms\", \"5s\", \"3m\", ...)")
		os.Exit(1)
	}

	controllerOptions := controller.Options{
		MaxConcurrentReconciles: maxConcurrentReconciles,
		RateLimiter:             controllers.NewFluidControllerRateLimiter(defaultSyncBackoff, maxSyncBackoff, controllerWorkqueueQPS, controllerWorkqueueBurst),
	}

	if err = (alluxioctl.NewRuntimeReconciler(mgr.GetClient(),
		ctrl.Log.WithName("alluxioctl").WithName("AlluxioRuntime"),
		mgr.GetScheme(),
		mgr.GetEventRecorderFor("AlluxioRuntime"),
	)).SetupWithManager(mgr, controllerOptions); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AlluxioRuntime")
		os.Exit(1)
	}

	pr, err := net.ParsePortRange(portRange)
	if err != nil {
		setupLog.Error(err, "can't parse port range. Port range must be like <min>-<max>")
		os.Exit(1)
	}
	setupLog.Info("port range parsed", "port range", pr.String())

	err = portallocator.SetupRuntimePortAllocator(mgr.GetClient(), pr, portAllocatePolicy, alluxio.GetReservedPorts)
	if err != nil {
		setupLog.Error(err, "failed to setup runtime port allocator")
		os.Exit(1)
	}
	setupLog.Info("Set up runtime port allocator", "policy", portAllocatePolicy)

	setupLog.Info("starting alluxioruntime-controller")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem alluxioruntime-controller")
		os.Exit(1)
	}
}
