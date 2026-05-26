// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 日志平台 (BlueKing - Log) available.
// Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
//

package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/containerd/containerd"
	"github.com/containerd/errdefs"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/define"
)

var (
	errContainerPIDNotReady = errors.New("container pid is not ready")
	errContainerNotRunning  = errors.New("container is not running")
)

// criClient wraps v1.RuntimeServiceClient for container listing and inspection.
// The actual CRI version (v1 vs v1alpha2) is determined by the gRPC connection's
// interceptor, not by this struct — the protobuf wire format is identical.
type criClient struct {
	client v1.RuntimeServiceClient
}

func (c *criClient) ListContainers(ctx context.Context) ([]define.SimpleContainer, error) {
	resp, err := c.client.ListContainers(ctx, &v1.ListContainersRequest{
		Filter: &v1.ContainerFilter{
			State: &v1.ContainerStateValue{
				State: v1.ContainerState_CONTAINER_RUNNING,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var result []define.SimpleContainer
	for _, container := range resp.GetContainers() {
		if container == nil {
			continue
		}
		result = append(result, define.SimpleContainer{ID: container.Id})
	}
	return result, nil
}

func (c *criClient) ContainerStatus(ctx context.Context, containerID string) (*v1.ContainerStatusResponse, error) {
	return c.client.ContainerStatus(ctx, &v1.ContainerStatusRequest{
		ContainerId: containerID,
	})
}

// ContainerdRuntime implements define.Runtime for all containerd versions.
// CRI v1 vs v1alpha2 is handled at the gRPC connection level via interceptor.
type ContainerdRuntime struct {
	ContainerdBase
	cri *criClient
}

func (r *ContainerdRuntime) Containers(ctx context.Context) ([]define.SimpleContainer, error) {
	return r.cri.ListContainers(ctx)
}

func pendingTaskError(err error, status *v1.ContainerStatusResponse, containerID, action string) error {
	if status.GetStatus().GetState() == v1.ContainerState_CONTAINER_EXITED {
		return fmt.Errorf("%w: container [%s] CRI state is exited", errContainerNotRunning, containerID)
	}
	if errdefs.IsNotFound(err) {
		return fmt.Errorf("%w: %s for container [%s]: %v", errContainerPIDNotReady, action, containerID, err)
	}
	return err
}

func containerPIDStateError(status containerd.ProcessStatus, containerID string) error {
	switch status {
	case containerd.Created:
		return fmt.Errorf("%w: container [%s] task state is [%s]", errContainerPIDNotReady, containerID, status)
	case containerd.Running, containerd.Paused, containerd.Pausing:
		return nil
	default:
		return fmt.Errorf("%w: container [%s] task state is [%s]", errContainerNotRunning, containerID, status)
	}
}

func (r *ContainerdRuntime) containerPID(ctx context.Context, containerID string, status *v1.ContainerStatusResponse) (int, error) {
	container, err := r.containerdClient.LoadContainer(ctx, containerID)
	if err != nil {
		return 0, pendingTaskError(err, status, containerID, "load container")
	}

	task, err := container.Task(ctx, nil)
	if err != nil {
		return 0, pendingTaskError(err, status, containerID, "get task")
	}

	taskStatus, err := task.Status(ctx)
	if err != nil {
		return 0, pendingTaskError(err, status, containerID, "get task status")
	}
	if err := containerPIDStateError(taskStatus.Status, containerID); err != nil {
		return 0, err
	}

	pid := task.Pid()
	if pid == 0 {
		return 0, fmt.Errorf("%w: container [%s]", errContainerPIDNotReady, containerID)
	}
	return int(pid), nil
}

// ResolveRootPath returns the process-root path used only by container file collection.
func (r *ContainerdRuntime) ResolveRootPath(ctx context.Context, containerID string) (string, error) {
	if !requiresContainerdPID() {
		return resolveContainerdRootPath(0)
	}

	containerStatus, err := r.cri.ContainerStatus(ctx, containerID)
	if err != nil {
		return "", err
	}
	pid, err := r.containerPID(ctx, containerID, containerStatus)
	if err != nil {
		return "", err
	}
	return resolveContainerdRootPath(pid)
}

func (r *ContainerdRuntime) Inspect(ctx context.Context, containerID string) (define.Container, error) {
	containerStatus, err := r.cri.ContainerStatus(ctx, containerID)
	if err != nil {
		return define.Container{}, err
	}

	var mounts []define.Mount
	for _, mount := range containerStatus.Status.Mounts {
		mounts = append(mounts, define.Mount{
			HostPath:      mount.HostPath,
			ContainerPath: mount.ContainerPath,
		})
	}

	logPath := containerStatus.Status.LogPath
	realLogPath, err := define.EvalSymlinks(logPath)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("container [%s] failed to eval symlink for log path [%s]", containerID, logPath))
	} else {
		logPath = realLogPath
	}

	// 获取不到镜像名称时使用 Image ID
	image := containerStatus.Status.ImageRef
	if containerStatus.Status.Image != nil {
		image = containerStatus.Status.Image.Image
	}
	return define.Container{
		ID:      containerStatus.Status.Id,
		Labels:  containerStatus.Status.Labels,
		Image:   image,
		LogPath: logPath,
		Mounts:  mounts,
	}, nil
}
