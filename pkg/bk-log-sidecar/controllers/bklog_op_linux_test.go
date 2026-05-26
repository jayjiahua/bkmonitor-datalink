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

	"github.com/stretchr/testify/assert"
)

func TestResolveContainerdRootPathDoesNotUseStatePathFallback(t *testing.T) {
	rootPath, err := resolveContainerdRootPath(0)

	assert.Empty(t, rootPath)
	assert.True(t, errors.Is(err, errContainerPIDNotReady))

	rootPath, err = resolveContainerdRootPath(1234)
	assert.NoError(t, err)
	assert.Equal(t, "/proc/1234/root", rootPath)
}
