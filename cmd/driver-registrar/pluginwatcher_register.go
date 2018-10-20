/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"net"
	"os"
	"os/signal"

	"google.golang.org/grpc"

	"github.com/golang/glog"
	"golang.org/x/sys/unix"
	crdclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	k8scsi "k8s.io/csi-api/pkg/apis/csi/v1alpha1"
	k8scsiclient "k8s.io/csi-api/pkg/client/clientset/versioned"
	k8scsicrd "k8s.io/csi-api/pkg/crd"
	registerapi "k8s.io/kubernetes/pkg/kubelet/apis/pluginregistration/v1alpha1"

	"github.com/kubernetes-csi/driver-registrar/pkg/connection"
)

func pluginWatcherRegister(
	config *rest.Config,
	csiConn connection.CSIConnection,
	csiDriverName string,
) {
	if *kubeletRegistrationPath == "" {
		glog.Fatalln("kubelet registration path not specified")
	}

	csiClientset, err := k8scsiclient.NewForConfig(config)
	if err != nil {
		glog.Error(err.Error())
		os.Exit(1)
	}

	// Register CRD for CSIDriver

	if err := registerCRD(config); err != nil {
		glog.Error(err.Error())
		os.Exit(1)
	}

	// Create a CSIDriver CRD object

	if err := createCSIDriverCRD(csiClientset, csiDriverName); err != nil {
		glog.Error(err.Error())
		os.Exit(1)
	}

	// Set up goroutine to cleanup (aka deregister) on termination.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		deleteCSIDriverCRD(csiClientset, csiDriverName)
		os.Exit(1)
	}()

	// Register the CSI driver using the kubelet plugin watcher machanism

	registrar := newRegistrationServer(csiDriverName, *kubeletRegistrationPath, supportedVersions)
	socketPath := fmt.Sprintf("/registration/%s-reg.sock", csiDriverName)
	fi, err := os.Stat(socketPath)
	if err == nil && (fi.Mode()&os.ModeSocket) != 0 {
		// Remove any socket, stale or not, but fall through for other files
		if err := os.Remove(socketPath); err != nil {
			glog.Errorf("failed to remove stale socket %s with error: %+v", socketPath, err)
			os.Exit(1)
		}
	}
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("failed to stat the socket %s with error: %+v", socketPath, err)
		os.Exit(1)
	}
	// Default to only user accessible socket, caller can open up later if desired
	oldmask := unix.Umask(0077)

	glog.Infof("Starting Registration Server at: %s\n", socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		glog.Errorf("failed to listen on socket: %s with error: %+v", socketPath, err)
		os.Exit(1)
	}
	unix.Umask(oldmask)
	glog.Infof("Registration Server started at: %s\n", socketPath)
	grpcServer := grpc.NewServer()
	// Registers kubelet plugin watcher api.
	registerapi.RegisterRegistrationServer(grpcServer, registrar)

	// Starts service
	if err := grpcServer.Serve(lis); err != nil {
		glog.Errorf("Registration Server stopped serving: %v", err)
		os.Exit(1)
	}
	// If gRPC server is gracefully shutdown, exit
	os.Exit(0)
}

func registerCRD(config *rest.Config) error {
	glog.V(1).Info("Registering " + k8scsi.CsiDriverResourcePlural)

	crdclient, err := crdclient.NewForConfig(config)
	if err != nil {
		return err
	}

	crdv1beta1client := crdclient.ApiextensionsV1beta1().CustomResourceDefinitions()
	_, err = crdv1beta1client.Create(k8scsicrd.CSIDriverCRD())

	if err == nil {
		glog.V(1).Info("CSIDriver CRD registered")
	} else if apierrors.IsAlreadyExists(err) {
		glog.V(1).Info("CSIDriver CRD already had been registered")
		return nil
	}

	return err
}

func createCSIDriverCRD(csiClientset *k8scsiclient.Clientset, csiDriverName string) error {
	glog.V(1).Infof("AttachRequired: %v", *k8sAttachmentRequired)
	glog.V(1).Infof("PodInfoOnMountVersion: %v", *k8sPodInfoOnMountVersion)

	csiDriver := &k8scsi.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: csiDriverName,
		},
		Spec: k8scsi.CSIDriverSpec{
			AttachRequired:        k8sAttachmentRequired,
			PodInfoOnMountVersion: k8sPodInfoOnMountVersion,
		},
	}

	csidrivers := csiClientset.CsiV1alpha1().CSIDrivers()

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := csidrivers.Create(csiDriver)
		if err == nil {
			glog.V(1).Infof("CSIDRiver object created for driver %s", csiDriverName)
			return nil
		} else if apierrors.IsAlreadyExists(err) {
			return nil
		} else {
			glog.Errorf("Failed to create CSIDriver object: %v", err)
			return err
		}
	})

	return retryErr
}

func deleteCSIDriverCRD(csiClientset *k8scsiclient.Clientset, csiDriverName string) error {
	csidrivers := csiClientset.CsiV1alpha1().CSIDrivers()

	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		err := csidrivers.Delete(csiDriverName, &metav1.DeleteOptions{})
		if err == nil {
			glog.V(1).Infof("CSIDRiver object deleted for driver %s", csiDriverName)
			return nil
		} else if apierrors.IsNotFound(err) {
			glog.V(1).Info("No need to clean up CSIDriver since it does not exist")
			return nil
		} else {
			glog.Errorf("Failed to create CSIDriver object: %v", err)
			return err
		}
	})

	return retryErr
}
