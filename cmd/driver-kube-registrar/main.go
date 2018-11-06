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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang/glog"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kubernetes-csi/driver-registrar/pkg/connection"
)

const (
	// Default timeout of short CSI calls like GetPluginInfo
	csiTimeout = time.Second

	// Verify (and update, if needed) the node ID at this freqeuency.
	sleepDuration = 2 * time.Minute
)

// Command line flags
var (
	kubeconfig            = flag.String("kubeconfig", "", "Absolute path to the kubeconfig file. Required only when running out of cluster.")
	k8sAttachmentRequired = flag.Bool("driver-requires-attachment",
		true,
		"Indicates this CSI volume driver requires an attach operation (because it "+
			"implements the CSI ControllerPublishVolume() method), and that Kubernetes "+
			"should call attach and wait for any attach operation to complete before "+
			"proceeding to mounting. If value is not specified, default is false meaning "+
			"attach will not be called.")
	k8sPodInfoOnMountVersion = flag.String("pod-info-mount-version",
		"",
		"This indicates that the associated CSI volume driver"+
			"requires additional pod information (like podName, podUID, etc.) during mount."+
			"A version of value \"v1\" will cause the Kubelet send the followings pod information "+
			"during NodePublishVolume() calls to the driver as VolumeAttributes:"+
			"- csi.storage.k8s.io/pod.name: pod.Name\n"+
			"- csi.storage.k8s.io/pod.namespace: pod.Namespace\n"+
			"- csi.storage.k8s.io/pod.uid: string(pod.UID)",
	)
	connectionTimeout = flag.Duration("connection-timeout", 1*time.Minute, "Timeout for waiting for CSI driver socket.")
	csiAddress        = flag.String("csi-address", "/run/csi/socket", "Address of the CSI driver socket.")
	showVersion       = flag.Bool("version", false, "Show version.")
	version           = "unknown"
	// List of supported versions
	supportedVersions = []string{"0.2.0", "0.3.0"}
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	if *showVersion {
		fmt.Println(os.Args[0], version)
		return
	}
	glog.Infof("Version: %s", version)

	// Connect to CSI.
	glog.V(1).Infof("Attempting to open a gRPC connection with: %q", *csiAddress)
	csiConn, err := connection.NewConnection(*csiAddress, *connectionTimeout)
	if err != nil {
		glog.Error(err.Error())
		os.Exit(1)
	}

	// Get CSI driver name.
	glog.V(1).Infof("Calling CSI driver to discover driver name.")
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	csiDriverName, err := csiConn.GetDriverName(ctx)
	if err != nil {
		glog.Error(err.Error())
		os.Exit(1)
	}
	glog.V(2).Infof("CSI driver name: %q", csiDriverName)

	// Create the client config. Use kubeconfig if given, otherwise assume
	// in-cluster.
	glog.V(1).Infof("Loading kubeconfig.")
	config, err := buildConfig(*kubeconfig)
	if err != nil {
		glog.Error(err.Error())
		os.Exit(1)
	}

	// Run forever
	kubernetesRegister(config, csiConn, csiDriverName)
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	// Return config object which uses the service account kubernetes gives to
	// pods. It's intended for clients that are running inside a pod running on
	// kubernetes.
	return rest.InClusterConfig()
}
