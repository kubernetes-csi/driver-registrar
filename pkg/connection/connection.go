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

package connection

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// CSIConnection is gRPC connection to a remote CSI driver and abstracts all
// CSI calls.
type CSIConnection interface {
	// GetDriverName returns driver name as discovered by GetPluginInfo() gRPC
	// call.
	GetDriverName(ctx context.Context) (string, error)

	// NodeGetId returns node ID of the current according to the CSI driver.
	NodeGetId(ctx context.Context) (string, error)

	// Close the connection
	Close() error
}

type csiConnection struct {
	conn *grpc.ClientConn
}

var (
	_ CSIConnection = &csiConnection{}
)

func NewConnection(
	address string, timeout time.Duration) (CSIConnection, error) {
	conn, err := connect(address, timeout)
	if err != nil {
		return nil, err
	}
	return &csiConnection{
		conn: conn,
	}, nil
}

func connect(address string, timeout time.Duration) (*grpc.ClientConn, error) {
	glog.V(2).Infof("Connecting to %s", address)

	dialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithUnaryInterceptor(logGRPC),
		grpc.WithBlock(), // blocks for conn until ctx.Timeout||cancel
	}
	if strings.HasPrefix(address, "/") {
		dialOptions = append(dialOptions, grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
	}

	// Backoff used to retry connection
	retryBackoff := wait.Backoff{
		Steps:    5,                      // number of tries
		Duration: 100 * time.Millisecond, // wait between tries
		Factor:   1.5,                    // backoff factor
		Jitter:   0.1,                    // uniformity between steps
	}

	var conn *grpc.ClientConn
	connErr := wait.ExponentialBackoff(retryBackoff, func() (bool, error) {
		glog.V(4).Infof("Registrar attempting to connect to %s...", address)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		c, err := grpc.DialContext(ctx, address, dialOptions...)
		if err != nil {
			glog.Errorf("Failed to connect to %s: %v", address, err)
			if isErrorRetryable(ctx, err) {
				glog.V(4).Infof("Attempting to reconnect to %s...", address)
				return false, nil
			}
			return false, err
		}
		conn = c
		return true, nil
	})

	if connErr != nil {
		return nil, fmt.Errorf("Connection attempts exhausted: %v", connErr)
	}

	return conn, nil
}

func (c *csiConnection) GetDriverName(ctx context.Context) (string, error) {
	client := csi.NewIdentityClient(c.conn)

	req := csi.GetPluginInfoRequest{}

	rsp, err := client.GetPluginInfo(ctx, &req)
	if err != nil {
		return "", err
	}
	name := rsp.GetName()
	if name == "" {
		return "", fmt.Errorf("name is empty")
	}
	return name, nil
}

func (c *csiConnection) NodeGetId(ctx context.Context) (string, error) {
	client := csi.NewNodeClient(c.conn)

	req := csi.NodeGetInfoRequest{}

	rsp, err := client.NodeGetInfo(ctx, &req)
	if err != nil {
		return "", err
	}
	nodeID := rsp.GetNodeId()
	if nodeID == "" {
		return "", fmt.Errorf("node ID is empty")
	}
	return nodeID, nil
}

func (c *csiConnection) Close() error {
	return c.conn.Close()
}

func logGRPC(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	glog.V(5).Infof("GRPC call: %s", method)
	glog.V(5).Infof("GRPC request: %+v", req)
	err := invoker(ctx, method, req, reply, cc, opts...)
	glog.V(5).Infof("GRPC response: %+v", reply)
	glog.V(5).Infof("GRPC error: %v", err)
	return err
}

// isErrorRetryable returns true if the tested error is transient and could be recovered from
// for given operation.  A retryable error means the operation that produced should be retried.
// It returns false, if the error is final and the operation should not be tried.
func isErrorRetryable(ctx context.Context, err error) (retryable bool) {
	if ctx.Err() == context.DeadlineExceeded {
		retryable = true
		return
	}

	st, ok := status.FromError(err)

	// if not status error, is it network-related?
	if !ok {
		switch e := err.(type) {
		case net.Error:
			if e.Temporary() || e.Timeout() {
				retryable = true
			} else {
				retryable = false
			}
		default:
			retryable = false // probaly not grpc-related
		}
		return
	}

	// grpc-related, is it recoverable
	switch st.Code() {
	case codes.Canceled,
		codes.Internal,
		codes.Unimplemented,
		codes.Unknown:
		retryable = false
	default:
		retryable = true
	}

	return
}
