// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package ratelimiter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/mapstructure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/ratelimiter"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
)

func TestFactory(t *testing.T) {
	content := `
processor:
  - name: "rate_limiter/token_bucket"
    config:
      type: token_bucket
      qps: 5
      burst: 10
`
	psc := processor.MustLoadConfigs(content)
	obj, err := NewFactory(psc[0].Config, nil)
	factory := obj.(*rateLimiter)
	assert.NoError(t, err)
	assert.Equal(t, psc[0].Config, factory.MainConfig())

	var c ratelimiter.Config
	err = mapstructure.Decode(psc[0].Config, &c)
	assert.NoError(t, err)
	assert.Equal(t, c, factory.configs.GetGlobal().(ratelimiter.Config))

	assert.Equal(t, define.ProcessorRateLimiter, factory.Name())
	assert.False(t, factory.IsDerived())
	assert.True(t, factory.IsPreCheck())

	factory.Reload(psc[0].Config, nil)
	assert.Equal(t, psc[0].Config, factory.MainConfig())
}

func TestNormalProcess(t *testing.T) {
	content := `
processor:
  - name: "rate_limiter/token_bucket"
    config:
      type: token_bucket
      qps: 5
      burst: 10
`
	factory := processor.MustCreateFactory(content, NewFactory)

	_, err := factory.Process(&define.Record{Token: define.Token{Original: "fortest"}})
	assert.NoError(t, err)
}

func TestDropAll(t *testing.T) {
	content := `
processor:
  - name: "rate_limiter/token_bucket"
    config:
      type: token_bucket
      qps: -1
`
	factory := processor.MustCreateFactory(content, NewFactory)

	_, err := factory.Process(&define.Record{Token: define.Token{Original: "fortest"}})
	assert.Error(t, err)
}
