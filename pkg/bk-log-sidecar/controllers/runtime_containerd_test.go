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
	"errors"
	"testing"

	"github.com/containerd/containerd"
	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestPendingTaskErrorOnlyRetriesNotFound(t *testing.T) {
	runningStatus := &v1.ContainerStatusResponse{
		Status: &v1.ContainerStatus{State: v1.ContainerState_CONTAINER_RUNNING},
	}
	err := pendingTaskError(errdefs.ErrNotFound, runningStatus, "container-1", "get task")
	assert.ErrorIs(t, err, errContainerPIDNotReady)

	permissionErr := errors.New("permission denied")
	err = pendingTaskError(permissionErr, runningStatus, "container-1", "get task")
	assert.Same(t, permissionErr, err)

	exitedStatus := &v1.ContainerStatusResponse{
		Status: &v1.ContainerStatus{State: v1.ContainerState_CONTAINER_EXITED},
	}
	err = pendingTaskError(permissionErr, exitedStatus, "container-1", "get task")
	assert.ErrorIs(t, err, errContainerNotRunning)
}

func TestContainerPIDStateAllowsLiveProcessStates(t *testing.T) {
	assert.NoError(t, containerPIDStateError(containerd.Running, "container-1"))
	assert.NoError(t, containerPIDStateError(containerd.Paused, "container-1"))
	assert.NoError(t, containerPIDStateError(containerd.Pausing, "container-1"))
	assert.ErrorIs(t, containerPIDStateError(containerd.Created, "container-1"), errContainerPIDNotReady)
	assert.ErrorIs(t, containerPIDStateError(containerd.Stopped, "container-1"), errContainerNotRunning)
}
