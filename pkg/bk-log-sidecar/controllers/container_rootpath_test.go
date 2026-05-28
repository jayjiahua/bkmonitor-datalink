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
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/define"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootPathRetryDuration(t *testing.T) {
	sidecar := &BkLogSidecar{}
	assert.Equal(t, time.Minute, sidecar.rootPathRetryDuration())

	sidecar = &BkLogSidecar{
		rootPathRetryInterval: time.Hour,
	}
	assert.Equal(t, time.Hour, sidecar.rootPathRetryDuration())
}

func TestRequeuePendingRootPathEventsUsesRunningListOnce(t *testing.T) {
	eventQueue := make(chan *containerEvent, 2)
	runtime := &rootPathRetryRuntime{
		containers: []define.SimpleContainer{{ID: "running-container"}},
	}
	sidecar := &BkLogSidecar{
		runtime:             runtime,
		containerEventQueue: eventQueue,
	}

	sidecar.storePendingRootPathEvent(&containerEvent{
		ContainerEvent: &define.ContainerEvent{
			Type:        define.ContainerEventCreate,
			ContainerID: "running-container",
		},
		isNewContainer: true,
	})
	sidecar.storePendingRootPathEvent(&containerEvent{
		ContainerEvent: &define.ContainerEvent{
			Type:        define.ContainerEventCreate,
			ContainerID: "stopped-container",
		},
		isNewContainer: true,
	})

	sidecar.requeuePendingRootPathEvents()

	assert.Equal(t, 1, runtime.containersCalls)
	require.Len(t, eventQueue, 1)

	event := <-eventQueue
	assert.Equal(t, "running-container", event.ContainerID)
	assert.True(t, event.isNewContainer)
	assert.True(t, event.rootPathRetry)

	_, ok := sidecar.pendingRootPathEvents.Load("running-container")
	assert.True(t, ok)
	_, ok = sidecar.pendingRootPathEvents.Load("stopped-container")
	assert.False(t, ok)
}

func TestRequeuePendingRootPathEventsSkipsRunningListWhenNoPending(t *testing.T) {
	sidecar := &BkLogSidecar{
		runtime:             &rootPathRetryRuntime{},
		containerEventQueue: make(chan *containerEvent, 1),
	}

	sidecar.requeuePendingRootPathEvents()

	assert.Equal(t, 0, sidecar.runtime.(*rootPathRetryRuntime).containersCalls)
}

func TestRootPathRetryEventSkipsWhenPendingWasCleared(t *testing.T) {
	sidecar := &BkLogSidecar{}

	sidecar.startActionHandler(newContainerEvent("container-1", true, true))

	assert.False(t, sidecar.hasPendingRootPathEvent("container-1"))
}

type rootPathRetryRuntime struct {
	containers      []define.SimpleContainer
	containersCalls int
}

func (r *rootPathRetryRuntime) Containers(context.Context) ([]define.SimpleContainer, error) {
	r.containersCalls++
	return r.containers, nil
}

func (r *rootPathRetryRuntime) Inspect(context.Context, string) (define.Container, error) {
	return define.Container{}, nil
}

func (r *rootPathRetryRuntime) Subscribe(context.Context) (<-chan *define.ContainerEvent, <-chan error) {
	return nil, nil
}

func (r *rootPathRetryRuntime) Type() define.RuntimeType {
	return define.RuntimeTypeContainerd
}
