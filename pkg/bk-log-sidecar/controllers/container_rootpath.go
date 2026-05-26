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
)

const defaultRootPathRetryInterval = 5 * time.Second

type rootPathResolver interface {
	ResolveRootPath(ctx context.Context, containerID string) (string, error)
}

type rootPathRetry struct {
	cancel         context.CancelFunc
	isNewContainer bool
}

func (s *BkLogSidecar) resolveContainerRootPath(container *define.Container) error {
	if container.RootPath != "" {
		return nil
	}
	resolver, ok := s.getRuntime().(rootPathResolver)
	if !ok {
		return nil
	}

	rootPath, err := resolver.ResolveRootPath(context.Background(), container.ID)
	if err != nil {
		return err
	}
	container.RootPath = rootPath
	return nil
}

func (s *BkLogSidecar) scheduleRootPathRetry(containerID string, isNewContainer bool) {
	s.retryMu.Lock()
	if s.pendingRootPathRetry == nil {
		s.pendingRootPathRetry = make(map[string]*rootPathRetry)
	}
	if retry, ok := s.pendingRootPathRetry[containerID]; ok {
		if isNewContainer {
			retry.isNewContainer = true
		}
		s.retryMu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	retry := &rootPathRetry{cancel: cancel, isNewContainer: isNewContainer}
	s.pendingRootPathRetry[containerID] = retry
	s.retryMu.Unlock()

	s.log.Info(fmt.Sprintf("container [%s] root path is not ready, retry every [%s]", containerID, s.rootPathRetryDuration()))
	go s.retryRootPath(ctx, containerID, retry)
}

func (s *BkLogSidecar) retryRootPath(ctx context.Context, containerID string, retry *rootPathRetry) {
	ticker := time.NewTicker(s.rootPathRetryDuration())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			value, ok := s.containerCache.Load(containerID)
			if !ok {
				s.cancelRootPathRetry(containerID)
				return
			}
			container := castContainer(value)
			err := s.resolveContainerRootPath(container)
			if errors.Is(err, errContainerPIDNotReady) {
				continue
			}
			if err != nil {
				s.log.Error(err, fmt.Sprintf("retry root path for container [%s] failed", containerID))
				s.cancelRootPathRetry(containerID)
				return
			}

			isNewContainer, ok := s.completeRootPathRetry(containerID, retry)
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

func (s *BkLogSidecar) rootPathRetryDuration() time.Duration {
	if s.rootPathRetryInterval > 0 {
		return s.rootPathRetryInterval
	}
	return defaultRootPathRetryInterval
}

func (s *BkLogSidecar) completeRootPathRetry(containerID string, retry *rootPathRetry) (bool, bool) {
	s.retryMu.Lock()
	defer s.retryMu.Unlock()

	current, ok := s.pendingRootPathRetry[containerID]
	if !ok || current != retry {
		return false, false
	}
	delete(s.pendingRootPathRetry, containerID)
	return current.isNewContainer, true
}

func (s *BkLogSidecar) cancelRootPathRetry(containerID string) {
	s.retryMu.Lock()
	retry, ok := s.pendingRootPathRetry[containerID]
	if ok {
		delete(s.pendingRootPathRetry, containerID)
	}
	s.retryMu.Unlock()
	if ok {
		retry.cancel()
	}
}

func (s *BkLogSidecar) cancelAllRootPathRetries() {
	s.retryMu.Lock()
	retries := s.pendingRootPathRetry
	s.pendingRootPathRetry = make(map[string]*rootPathRetry)
	s.retryMu.Unlock()

	for _, retry := range retries {
		retry.cancel()
	}
}
