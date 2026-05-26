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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/utils"
)

const defaultContainerRetryInterval = 5 * time.Second

type containerRetry struct {
	cancel         context.CancelFunc
	isNewContainer bool
}

func castContainer(c interface{}) *define.Container {
	return c.(*define.Container)
}

func (s *BkLogSidecar) periodCacheContainer() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.cacheContainer()
		case <-s.stopCh:
			s.log.Info("stop periodCacheContainer")
			return
		}
	}
}

func (s *BkLogSidecar) cacheContainer() {
	s.log.Info("cache container info start")
	ctx := context.Background()
	containers, err := s.getRuntime().Containers(ctx)
	if utils.NotNil(err) {
		s.log.Error(err, "list container failed")
		return
	}

	for _, container := range containers {
		containerInfo, err := s.containerByID(container.ID)
		if err != nil {
			if errors.Is(err, errContainerPIDNotReady) {
				s.scheduleContainerRetry(container.ID, false)
			}
			continue
		}
		s.containerCache.Store(container.ID, containerInfo)
	}
	s.log.Info("cache container info end")
}

func (s *BkLogSidecar) getContainerInfoByID(containerID string) (*define.Container, error) {
	containerInfo, ok := s.containerCache.Load(containerID)
	if ok {
		return castContainer(containerInfo), nil
	}

	container, err := s.containerByID(containerID)
	if err != nil {
		return nil, err
	}
	s.containerCache.Store(containerID, container)
	return container, nil
}

func (s *BkLogSidecar) containerByID(containerID string) (*define.Container, error) {
	ctx := context.Background()
	container, err := s.getRuntime().Inspect(ctx, containerID)
	if err != nil {
		s.log.Info(fmt.Sprintf("get container by id [%s] error: %s", containerID, err))
		return nil, err
	}
	return &container, nil
}

func (s *BkLogSidecar) scheduleContainerRetry(containerID string, isNewContainer bool) {
	s.retryMu.Lock()
	if s.pendingContainerRetry == nil {
		s.pendingContainerRetry = make(map[string]*containerRetry)
	}
	if retry, ok := s.pendingContainerRetry[containerID]; ok {
		if isNewContainer {
			retry.isNewContainer = true
		}
		s.retryMu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	retry := &containerRetry{cancel: cancel, isNewContainer: isNewContainer}
	s.pendingContainerRetry[containerID] = retry
	s.retryMu.Unlock()

	s.log.Info(fmt.Sprintf("container [%s] pid is not ready, retry every [%s]", containerID, s.retryInterval()))
	go s.retryContainer(ctx, containerID, retry)
}

func (s *BkLogSidecar) retryContainer(ctx context.Context, containerID string, retry *containerRetry) {
	ticker := time.NewTicker(s.retryInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			container, err := s.containerByID(containerID)
			if errors.Is(err, errContainerPIDNotReady) {
				continue
			}
			if err != nil {
				s.cancelContainerRetry(containerID)
				return
			}

			isNewContainer, ok := s.completeContainerRetry(containerID, retry)
			if !ok {
				return
			}
			s.applyContainerConfig(container, isNewContainer)
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *BkLogSidecar) retryInterval() time.Duration {
	if s.containerRetryInterval > 0 {
		return s.containerRetryInterval
	}
	return defaultContainerRetryInterval
}

func (s *BkLogSidecar) completeContainerRetry(containerID string, retry *containerRetry) (bool, bool) {
	s.retryMu.Lock()
	defer s.retryMu.Unlock()

	current, ok := s.pendingContainerRetry[containerID]
	if !ok || current != retry {
		return false, false
	}
	delete(s.pendingContainerRetry, containerID)
	return current.isNewContainer, true
}

func (s *BkLogSidecar) cancelContainerRetry(containerID string) {
	s.retryMu.Lock()
	retry, ok := s.pendingContainerRetry[containerID]
	if ok {
		delete(s.pendingContainerRetry, containerID)
	}
	s.retryMu.Unlock()
	if ok {
		retry.cancel()
	}
}

func (s *BkLogSidecar) cancelAllContainerRetries() {
	s.retryMu.Lock()
	retries := s.pendingContainerRetry
	s.pendingContainerRetry = make(map[string]*containerRetry)
	s.retryMu.Unlock()

	for _, retry := range retries {
		retry.cancel()
	}
}
