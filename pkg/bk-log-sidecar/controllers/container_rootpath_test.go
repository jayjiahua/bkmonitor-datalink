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
	"sync/atomic"
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

func TestScheduleRootPathRetryRequeuesRunningContainer(t *testing.T) {
	eventQueue := make(chan *containerEvent, 1)
	runtime := &rootPathRetryRuntime{
		containers: []define.SimpleContainer{{ID: "running-container"}},
	}
	sidecar := &BkLogSidecar{
		runtime:               runtime,
		containerEventQueue:   eventQueue,
		rootPathRetryInterval: time.Millisecond,
	}

	sidecar.scheduleRootPathRetry(newContainerEvent("running-container", true))

	select {
	case event := <-eventQueue:
		assert.Equal(t, "running-container", event.ContainerID)
		assert.True(t, event.isNewContainer)
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for root path retry event")
	}
	assert.Equal(t, int32(1), runtime.containersCalls.Load())
}

func TestScheduleRootPathRetryDropsStoppedContainer(t *testing.T) {
	eventQueue := make(chan *containerEvent, 1)
	runtime := &rootPathRetryRuntime{}
	sidecar := &BkLogSidecar{
		runtime:               runtime,
		containerEventQueue:   eventQueue,
		rootPathRetryInterval: time.Millisecond,
	}

	sidecar.scheduleRootPathRetry(newContainerEvent("stopped-container", true))

	select {
	case event := <-eventQueue:
		require.Failf(t, "unexpected root path retry event", "event: %v", event)
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, int32(1), runtime.containersCalls.Load())
}

type rootPathRetryRuntime struct {
	containers      []define.SimpleContainer
	containersCalls atomic.Int32
}

func (r *rootPathRetryRuntime) Containers(context.Context) ([]define.SimpleContainer, error) {
	r.containersCalls.Add(1)
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
