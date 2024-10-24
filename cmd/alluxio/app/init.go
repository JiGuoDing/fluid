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

import "github.com/spf13/cobra"

func NewAlluxioFSCommand() *cobra.Command {
	// 创建一个新的 Cobra 命令结构体实例
	cmd := &cobra.Command{
		// 指定命令的名称，用户在命令行中输入这个命令名称将对应于这个字段，用户可以通过alluxioruntime-controller调用这个命令
		Use: "alluxioruntime-controller",
		// 提供命令的简短描述
		Short: "Controller for alluxioruntime",
		// 定义该命令执行时运行的函数
		// Run: func(){}
	}

	// 添加子命令
	cmd.AddCommand(versionCmd, alluxioCmd)

	return cmd
}
