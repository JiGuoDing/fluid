package envtests

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	// * envtest 环境：管理本地模拟 k8s 集群的启停
	testEnv *envtest.Environment
	// * k8s REST 配置：包含 API Server 地址、认证信息
	cfg *rest.Config
	// * k8s 客户端：用于操作 CRD/核心资源 (如创建/查询 Dataset)
	k8sClient *client.Client
	// * 资源类型注册表 (注册Fluid CRD 和 K8s 核心资源)
	scheme *runtime.Scheme
	// * 全局上下文：无超时的默认上下文
	ctx = context.Background()
)

// Go 原生测试入口 (必须以 Test 开头)
func TestDatasetEnvTest(t *testing.T) {
	// * 配置测试日志，输出到 Ginkgo 控制台，启用开发模式(显示详细日志)
	logf.SetLogger(zap.New(zap.WriteTo(ginkgo.GinkgoWriter), zap.UseDevMode(true)))
	// * 注册 Gomega 失败处理器：将 Gomega 断言失败映射为 Go 测试失败
	gomega.RegisterFailHandler(ginkgo.Fail)
	// * 启动 Ginkgo 测试套件：第二个参数为测试套件名称
	ginkgo.RunSpecs(t, "Fluid Dataset EnvTest Suite")
}
