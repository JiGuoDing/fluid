/*
Copyright 2020 The Fluid Authors.

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

package alluxio

import (
	"context"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller"

	"sync"

	"github.com/pkg/errors"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/controllers"
	"github.com/fluid-cloudnative/fluid/pkg/ddc"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

const controllerName string = "AlluxioRuntimeController"

// Use compiler to check if the struct implements all the interface
// 确保 *RuntimeReconciler 类型确实实现了 controllers.RuntimeReconcilerInterface 接口
var _ controllers.RuntimeReconcilerInterface = (*RuntimeReconciler)(nil)

// RuntimeReconciler reconciles a AlluxioRuntime object
type RuntimeReconciler struct {
	Scheme  *runtime.Scheme
	engines map[string]base.Engine
	mutex   *sync.Mutex
	*controllers.RuntimeReconciler
}

// NewRuntimeReconciler create controller for watching runtime custom resources created
func NewRuntimeReconciler(client client.Client,
	log logr.Logger,
	scheme *runtime.Scheme,
	recorder record.EventRecorder) *RuntimeReconciler {
	r := &RuntimeReconciler{
		Scheme:  scheme,
		mutex:   &sync.Mutex{},
		engines: map[string]base.Engine{},
	}
	r.RuntimeReconciler = controllers.NewRuntimeReconciler(r, client, log, recorder)
	return r
}

// Reconcile reconciles alluxio runtime
// +kubebuilder:rbac:groups=data.fluid.io,resources=alluxioruntimes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=data.fluid.io,resources=alluxioruntimes/status,verbs=get;update;patch
//
// 两个参数,context 上下文对象，用于控制请求的超时和取消；req 一个请求对象，包含了要调和的资源的名称和命名空间
// 返回值ctrl.Result 用于指示是否需要重新排队和下一次调和
func (r *RuntimeReconciler) Reconcile(context context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 用于跟踪函数的执行时间
	defer utils.TimeTrack(time.Now(), "Reconcile", "request", req)
	// ctx 包含了调和过程中需要的各种上下文信息
	ctx := cruntime.ReconcileRequestContext{
		Context:        context,
		Log:            r.Log.WithValues("alluxioruntime", req.NamespacedName),
		NamespacedName: req.NamespacedName,
		Recorder:       r.Recorder,
		Category:       common.AccelerateCategory,
		RuntimeType:    common.AlluxioRuntime,
		Client:         r.Client,
		FinalizerName:  runtimeResourceFinalizerName,
	}

	ctx.Log.V(1).Info("process the request", "request", req)

	//	1.Load the Runtime
	//  加载运行时
	runtime, err := r.getRuntime(ctx)
	if err != nil {
		if utils.IgnoreNotFound(err) == nil {
			ctx.Log.V(1).Info("The runtime is not found", "runtime", ctx.NamespacedName)
			return ctrl.Result{}, nil
		} else {
			ctx.Log.Error(err, "Failed to get the ddc runtime")
			return utils.RequeueIfError(errors.Wrap(err, "Unable to get ddc runtime"))
		}
	}
	ctx.Runtime = runtime
	ctx.EngineImpl = ddc.InferEngineImpl(runtime.Status, common.AlluxioEngineImpl)
	ctx.Log.V(1).Info("process the runtime", "runtime", ctx.Runtime)

	// reconcile the implement
	return r.ReconcileInternal(ctx)
}

// SetupWithManager setups the manager with RuntimeReconciler
func (r *RuntimeReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(options).
		For(&datav1alpha1.AlluxioRuntime{}).
		Complete(r)
}

func (r *RuntimeReconciler) ControllerName() string {
	return controllerName
}
