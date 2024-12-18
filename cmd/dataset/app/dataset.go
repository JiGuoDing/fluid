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
	"os"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/spf13/cobra"
	zapOpt "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/fluid-cloudnative/fluid"
	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/controllers"
	databackupctl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/databackup"
	dataflowctl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/dataflow"
	dataloadctl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/dataload"
	datamigratectl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/datamigrate"
	dataprocessctl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/dataprocess"
	datasetctl "github.com/fluid-cloudnative/fluid/pkg/controllers/v1alpha1/dataset"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/alluxio"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/compatibility"
	"github.com/fluid-cloudnative/fluid/pkg/utils/discovery"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	// Use compiler to check if the struct implements all the interface
	_ base.Implement = (*alluxio.AlluxioEngine)(nil)

	metricsAddr             string
	enableLeaderElection    bool
	leaderElectionNamespace string
	development             bool
	pprofAddr               string
	maxConcurrentReconciles int

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

var datasetCmd = &cobra.Command{
	Use:   "start",
	Short: "start dataset-controller in Kubernetes",
	Run: func(cmd *cobra.Command, args []string) {
		handle()
	},
}

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = datav1alpha1.AddToScheme(scheme)

	datasetCmd.Flags().StringVarP(&metricsAddr, "metrics-addr", "", ":8080", "The address the metric endpoint binds to.")
	datasetCmd.Flags().BoolVarP(&enableLeaderElection, "enable-leader-election", "", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	datasetCmd.Flags().StringVarP(&leaderElectionNamespace, "leader-election-namespace", "", "fluid-system", "The namespace in which the leader election resource will be created.")
	datasetCmd.Flags().BoolVarP(&development, "development", "", true, "Enable development mode for fluid controller.")
	datasetCmd.Flags().StringVarP(&pprofAddr, "pprof-addr", "", "", "The address for pprof to use while exporting profiling results")
	datasetCmd.Flags().IntVar(&maxConcurrentReconciles, "reconcile-workers", 3, "Set the number of max concurrent workers for reconciling dataset and dataset operations")
	datasetCmd.Flags().Float32VarP(&kubeClientQPS, "kube-api-qps", "", 20, "QPS to use while talking with kubernetes apiserver.")   // 20 is the default qps in controller-runtime
	datasetCmd.Flags().IntVarP(&kubeClientBurst, "kube-api-burst", "", 30, "Burst to use while talking with kubernetes apiserver.") // 30 is the default burst in controller-runtime
	datasetCmd.Flags().StringVar(&controllerWorkqueueDefaultSyncBackoffStr, "workqueue-default-sync-backoff", "5ms", "base backoff period for failed reconciliation in controller's workqueue")
	datasetCmd.Flags().StringVar(&controllerWorkqueueMaxSyncBackoffStr, "workqueue-max-sync-backoff", "1000s", "max backoff period for failed reconciliation in controller's workqueue")
	datasetCmd.Flags().IntVar(&controllerWorkqueueQPS, "workqueue-qps", 10, "qps limit value for controller's workqueue")
	datasetCmd.Flags().IntVar(&controllerWorkqueueBurst, "workqueue-burst", 100, "burst limit value for controller's workqueue")
}

func handle() {
	fluid.LogVersion()
	// 获取fluid资源发现器
	fluidDiscovery := discovery.GetFluidDiscovery()

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

	// 如果提供了pprof地址，创建pprof服务器
	utils.NewPprofServer(setupLog, pprofAddr, development)

	// the default webhook server port is 9443, no need to set
	// 创建控制器管理器
	// 创建一个新的控制器管理器，使用给定的配置和选项
	mgr, err := ctrl.NewManager(controllers.GetConfigOrDieWithQPSAndBurst(kubeClientQPS, kubeClientBurst), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		LeaderElection:          enableLeaderElection,
		LeaderElectionNamespace: leaderElectionNamespace,
		LeaderElectionID:        "dataset.data.fluid.io",
		Cache:                   NewCacheOptions(),
		NewClient:               controllers.NewFluidControllerClient,
	})
	if err != nil {
		setupLog.Error(err, "unable to start dataset manager")
		os.Exit(1)
	}

	// 解析控制器工作队列的同步回退策略
	defaultSyncBackoff, err := time.ParseDuration(controllerWorkqueueDefaultSyncBackoffStr)
	if err != nil {
		setupLog.Error(err, "workqueue-default-sync-backoff is not a valid duration, please use string like \"100ms\", \"5s\", \"3m\", ...")
		os.Exit(1)
	}

	// 解析控制器工作队列的最大同步回退策略
	maxSyncBackoff, err := time.ParseDuration(controllerWorkqueueMaxSyncBackoffStr)
	if err != nil {
		setupLog.Error(err, "workqueue-max-sync-backoff is not a valid duration, please use string like \"100ms\", \"5s\", \"3m\", ...)")
		os.Exit(1)
	}

	// 设置控制器选项
	controllerOptions := controller.Options{
		MaxConcurrentReconciles: maxConcurrentReconciles,
		RateLimiter:             controllers.NewFluidControllerRateLimiter(defaultSyncBackoff, maxSyncBackoff, controllerWorkqueueQPS, controllerWorkqueueBurst),
	}

	// 注册Dataset的Reconciler
	setupLog.Info("Registering Dataset reconciler to Fluid controller manager.")
	if err = (&datasetctl.DatasetReconciler{
		Client:       mgr.GetClient(),
		Log:          ctrl.Log.WithName("datasetctl").WithName("Dataset"),
		Scheme:       mgr.GetScheme(),
		Recorder:     mgr.GetEventRecorderFor("Dataset"),
		ResyncPeriod: time.Duration(5 * time.Second),
	}).SetupWithManager(mgr, controllerOptions); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Dataset")
		os.Exit(1)
	}

	// 根据dataload是否启用，注册其Reconciler
	if fluidDiscovery.ResourceEnabled("dataload") {
		setupLog.Info("Registering DataLoad reconciler to Fluid controller manager.")
		if err = (dataloadctl.NewDataLoadReconciler(mgr.GetClient(),
			ctrl.Log.WithName("dataloadctl").WithName("DataLoad"),
			mgr.GetScheme(),
			mgr.GetEventRecorderFor("DataLoad"),
		)).SetupWithManager(mgr, controllerOptions); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "DataLoad")
			os.Exit(1)
		}
	}

	// 根据databackup是否启用，注册其Reconciler
	if fluidDiscovery.ResourceEnabled("databackup") {
		setupLog.Info("Registering DataBackup reconciler to Fluid controller manager.")
		if err = (databackupctl.NewDataBackupReconciler(mgr.GetClient(),
			ctrl.Log.WithName("databackupctl").WithName("DataBackup"),
			mgr.GetScheme(),
			mgr.GetEventRecorderFor("DataBackup"),
		)).SetupWithManager(mgr, controllerOptions); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "DataBackup")
			os.Exit(1)
		}
	}

	// 根据datamigrate是否启用，注册其Reconciler
	if fluidDiscovery.ResourceEnabled("datamigrate") {
		setupLog.Info("Registering DataMigrate reconciler to Fluid controller manager.")
		if err = (datamigratectl.NewDataMigrateReconciler(mgr.GetClient(),
			ctrl.Log.WithName("datamigratectl").WithName("DataMigrate"),
			mgr.GetScheme(),
			mgr.GetEventRecorderFor("DataMigrate"),
		)).SetupWithManager(mgr, controllerOptions); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "DataMigrate")
			os.Exit(1)
		}
	}

	// 根据dataprocess是否启用，注册其Reconciler
	if fluidDiscovery.ResourceEnabled("dataprocess") {
		setupLog.Info("Registering DataProcess reconciler to Fluid controller manager.")
		if err = (dataprocessctl.NewDataProcessReconciler(mgr.GetClient(),
			ctrl.Log.WithName("dataprocessctl").WithName("DataProcess"),
			mgr.GetScheme(),
			mgr.GetEventRecorderFor("DataProcess"),
		)).SetupWithManager(mgr, controllerOptions); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "DataProcess")
			os.Exit(1)
		}
	}

	// 根据dataflow是否启用，注册其Reconciler
	if dataflowctl.DataFlowEnabled() {
		setupLog.Info("Registering DataFlow reconciler to Fluid controller manager.")
		if err = (dataflowctl.NewDataFlowReconciler(mgr.GetClient(),
			ctrl.Log.WithName("dataflowctl"),
			mgr.GetEventRecorderFor("DataFlow"),
			time.Duration(5*time.Second),
		)).SetupWithManager(mgr, controllerOptions); err != nil {
			setupLog.Error(err, "unable to create controller")
			os.Exit(1)
		}
	}

	// 启动控制器管理器，开始监听和处理事件
	setupLog.Info("starting dataset-controller")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running dataset-controller")
		os.Exit(1)
	}
}

func NewCacheOptions() cache.Options {
	var cronJobKey client.Object

	if compatibility.IsBatchV1CronJobSupported() {
		cronJobKey = &batchv1.CronJob{}
	} else {
		cronJobKey = &batchv1beta1.CronJob{}
	}

	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			cronJobKey: {
				Label: labels.SelectorFromSet(labels.Set{
					common.JobPolicy: common.CronPolicy,
				}),
			},
		},
	}
}
