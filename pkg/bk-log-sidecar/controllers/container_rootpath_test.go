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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRootPathCheckDuration(t *testing.T) {
	sidecar := &BkLogSidecar{}
	assert.Equal(t, time.Minute, sidecar.rootPathCheckDuration())

	sidecar = &BkLogSidecar{
		rootPathCheckInterval: time.Hour,
	}
	assert.Equal(t, time.Hour, sidecar.rootPathCheckDuration())
}

func TestPendingRootPathNewContainerMarker(t *testing.T) {
	sidecar := &BkLogSidecar{}

	assert.False(t, sidecar.isPendingRootPathNewContainer("container-1"))

	sidecar.markPendingRootPathNewContainer("container-1")
	assert.True(t, sidecar.isPendingRootPathNewContainer("container-1"))

	sidecar.clearStoppedPendingRootPathNewContainers(map[string]struct{}{
		"container-1": {},
	})
	assert.True(t, sidecar.isPendingRootPathNewContainer("container-1"))

	sidecar.clearStoppedPendingRootPathNewContainers(map[string]struct{}{})
	assert.False(t, sidecar.isPendingRootPathNewContainer("container-1"))
}
