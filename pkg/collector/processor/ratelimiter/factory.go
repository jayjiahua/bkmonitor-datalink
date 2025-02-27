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
	"sync"

	"github.com/pkg/errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/confengine"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/mapstructure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/ratelimiter"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/processor"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

func init() {
	processor.Register(define.ProcessorRateLimiter, NewFactory)
}

func NewFactory(conf map[string]interface{}, customized []processor.SubConfigProcessor) (processor.Processor, error) {
	return newFactory(conf, customized)
}

func newFactory(conf map[string]interface{}, customized []processor.SubConfigProcessor) (*rateLimiter, error) {
	configs := confengine.NewTierConfig()

	var c ratelimiter.Config
	if err := mapstructure.Decode(conf, &c); err != nil {
		return nil, err
	}
	configs.SetGlobal(c)

	for _, custom := range customized {
		var cfg ratelimiter.Config
		if err := mapstructure.Decode(custom.Config.Config, &cfg); err != nil {
			logger.Errorf("failed to decode config: %v", err)
			continue
		}
		configs.Set(custom.Token, custom.Type, custom.ID, cfg)
	}

	return &rateLimiter{
		CommonProcessor: processor.NewCommonProcessor(conf, customized),
		configs:         configs,
		rateLimiters:    map[string]ratelimiter.RateLimiter{},
	}, nil
}

type rateLimiter struct {
	processor.CommonProcessor
	configs *confengine.TierConfig // type: Config

	mut          sync.Mutex
	rateLimiters map[string]ratelimiter.RateLimiter
}

func (p *rateLimiter) Name() string {
	return define.ProcessorRateLimiter
}

func (p *rateLimiter) IsDerived() bool {
	return false
}

func (p *rateLimiter) IsPreCheck() bool {
	return true
}

func (p *rateLimiter) Reload(config map[string]interface{}, customized []processor.SubConfigProcessor) {
	f, err := newFactory(config, customized)
	if err != nil {
		logger.Errorf("failed to reload processor: %v", err)
		return
	}

	p.CommonProcessor = f.CommonProcessor
	p.configs = f.configs
	p.rateLimiters = f.rateLimiters
}

func (p *rateLimiter) getRateLimiter(token string) ratelimiter.RateLimiter {
	c := p.configs.GetByToken(token).(ratelimiter.Config)
	p.mut.Lock()
	defer p.mut.Unlock()
	if _, ok := p.rateLimiters[token]; !ok {
		p.rateLimiters[token] = ratelimiter.New(c)
	}
	return p.rateLimiters[token]
}

func (p *rateLimiter) Process(record *define.Record) (*define.Record, error) {
	token := record.Token.Original
	rl := p.getRateLimiter(token)
	logger.Debugf("ratelimiter: token [%v] max qps allowed: %f", token, rl.QPS())
	if !rl.TryAccept() {
		return nil, errors.Errorf("ratelimiter rejected the request, token [%v] max qps allowed: %f", token, rl.QPS())
	}
	return nil, nil
}
