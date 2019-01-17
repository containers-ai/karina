/*

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

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/containers-ai/karina/operator"
	datahub_node "github.com/containers-ai/karina/operator/datahub/client/node"
	"github.com/containers-ai/karina/operator/pkg/apis"
	"github.com/containers-ai/karina/operator/pkg/controller"
	"github.com/containers-ai/karina/operator/pkg/utils/resources"
	"github.com/containers-ai/karina/operator/pkg/webhook"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	"github.com/spf13/viper"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/runtime/signals"
)

const (
	envVarPrefix = "KARINA"
	jsonIndent   = "  "
)

var scope *logUtil.Scope

var isLogOutput bool
var configFilePath string
var operatorConf operator.Config
var metricsAddr string

func init() {
	flag.BoolVar(&isLogOutput, "logfile", false, "output log file")
	flag.StringVar(&configFilePath, "config", "/etc/federatorai/operator/operator.yml", "File path to operator coniguration")
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")

	scope = logUtil.RegisterScope("manager", "operator entry point", 0)
}

func initLogger() {
	scope.Infof("Log output level is %s.", operatorConf.Log.OutputLevel)
	scope.Infof("Log stacktrace level is %s.", operatorConf.Log.StackTraceLevel)
	for _, scope := range logUtil.Scopes() {
		scope.SetLogCallers(operatorConf.Log.SetLogCallers == true)
		if outputLvl, ok := logUtil.StringToLevel(operatorConf.Log.OutputLevel); ok {
			scope.SetOutputLevel(outputLvl)
		}
		if stacktraceLevel, ok := logUtil.StringToLevel(operatorConf.Log.StackTraceLevel); ok {
			scope.SetStackTraceLevel(stacktraceLevel)
		}
	}
}

func initConfig() {

	flag.Parse()

	operatorConf = operator.NewDefaultConfig()

	viper.SetEnvPrefix(envVarPrefix)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AllowEmptyEnv(true)

	viper.SetConfigFile(configFilePath)
	err := viper.ReadInConfig()
	if err != nil {
		panic(errors.New("Read configuration failed: " + err.Error()))
	}
	err = viper.Unmarshal(&operatorConf)
	if err != nil {
		panic(errors.New("Unmarshal configuration failed: " + err.Error()))
	} else {
		if operatorConfBin, err := json.MarshalIndent(operatorConf, "", jsonIndent); err == nil {
			scope.Infof(fmt.Sprintf("Operator configuration: %s", string(operatorConfBin)))
		}
	}
}

func main() {

	initConfig()
	initLogger()
	operator.NewOperatorWithConfig(operatorConf)

	// Get a config to talk to the apiserver
	scope.Info("setting up client for manager")
	cfg, err := config.GetConfig()
	if err != nil {
		scope.Errorf("unable to set up client config: %s", err.Error())
		os.Exit(1)
	}

	// Create a new Cmd to provide shared dependencies and start components
	scope.Info("setting up manager")
	mgr, err := manager.New(cfg, manager.Options{MetricsBindAddress: operatorConf.MetricsAddress})
	if err != nil {
		scope.Errorf("unable to set up overall controller manager: %s", err.Error())
		os.Exit(1)
	}
	operatorConf.SetManager(mgr)

	scope.Info("Registering Components.")

	// Setup Scheme for all resources
	scope.Info("setting up scheme")
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		scope.Errorf("unable add APIs to scheme: %s", err.Error())
		os.Exit(1)
	}

	// Setup all Controllers
	scope.Info("Setting up controller")
	if err := controller.AddToManager(mgr); err != nil {
		scope.Errorf("unable to register controllers to the manager: %s", err.Error())
		os.Exit(1)
	}

	scope.Info("setting up webhooks")
	if err := webhook.AddToManager(mgr); err != nil {
		scope.Errorf("unable to register webhooks to the manager: %s", err.Error())
		os.Exit(1)
	}

	go registerNodes(mgr.GetClient())

	// Start the Cmd
	scope.Info("Starting the Cmd.")
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		scope.Errorf("unable to run the manager: %s", err.Error())
		os.Exit(1)
	}
}

func registerNodes(client client.Client) {
	time.Sleep(3 * time.Second)
	listResources := resources.NewListResources(client)
	nodeList, _ := listResources.ListAllNodes()
	scope.Infof(fmt.Sprintf("%v nodes found in cluster.", len(nodeList)))
	createNode := datahub_node.NewCreateNode()
	createNode.CreateNode(nodeList)
}
