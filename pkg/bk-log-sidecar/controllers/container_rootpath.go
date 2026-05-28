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

const defaultRootPathCheckInterval = time.Minute

type rootPathResolver interface {
	ResolveRootPath(ctx context.Context, containerID string) (string, error)
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

func (s *BkLogSidecar) initRootPathCheck() {
	go s.periodCheckRootPath()
}

func (s *BkLogSidecar) periodCheckRootPath() {
	s.checkRootPath()

	ticker := time.NewTicker(s.rootPathCheckDuration())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.checkRootPath()
		case <-s.stopCh:
			s.log.Info("stop periodCheckRootPath")
			return
		}
	}
}

func (s *BkLogSidecar) checkRootPath() {
	containers, err := s.allContainers()
	if err != nil {
		s.log.Error(err, "check root path list container failed")
		return
	}

	runningContainerIDs := make(map[string]struct{}, len(containers))
	var bkLogConfigs []define.LogConfigType
	for _, simpleContainer := range containers {
		runningContainerIDs[simpleContainer.ID] = struct{}{}
		container := s.getContainerInfoByID(simpleContainer.ID)
		if container == nil || !s.hasMissingContainerRootPathConfig(container) {
			continue
		}
		if err := s.resolveContainerRootPath(container); errors.Is(err, errContainerPIDNotReady) {
			continue
		} else if err != nil {
			s.log.Error(err, fmt.Sprintf("check root path for container [%s] failed", simpleContainer.ID))
			continue
		}

		isNewContainer := s.isPendingRootPathNewContainer(simpleContainer.ID)
		s.clearPendingRootPathNewContainer(simpleContainer.ID)

		var containerConfigs []define.LogConfigType
		containerConfigs, _ = s.containerBkLogConfigs(container, containerConfigs, isNewContainer)
		if define.Empty(containerConfigs) {
			continue
		}
		bkLogConfigs = append(bkLogConfigs, containerConfigs...)
	}
	s.clearStoppedPendingRootPathNewContainers(runningContainerIDs)

	if define.Empty(bkLogConfigs) {
		return
	}
	for _, logConfig := range bkLogConfigs {
		s.actualBkLogConfigCache.Store(logConfig.ConfigName(), logConfig)
	}
	s.writeConfig()
	if err := s.reloadBkunifylogbeat(); err != nil {
		s.log.Error(err, "check root path reload agent failed")
	}
}

func (s *BkLogSidecar) hasMissingContainerRootPathConfig(container *define.Container) bool {
	if container.RootPath != "" {
		return false
	}
	matchBklogConfigs, pod := s.matchBklogConfigs(container)
	for _, bkLogConfig := range matchBklogConfigs {
		if !bkLogConfig.IsContainerType() {
			continue
		}
		logConfig := &define.ContainerLogConfig{
			BkLogConfig: bkLogConfig,
			Container:   container,
			Pod:         pod,
		}
		if _, ok := s.actualBkLogConfigCache.Load(logConfig.ConfigName()); !ok {
			return true
		}
	}
	return false
}

func (s *BkLogSidecar) rootPathCheckDuration() time.Duration {
	if s.rootPathCheckInterval > 0 {
		return s.rootPathCheckInterval
	}
	return defaultRootPathCheckInterval
}

func (s *BkLogSidecar) markPendingRootPathNewContainer(containerID string) {
	s.pendingRootPathNew.Store(containerID, true)
}

func (s *BkLogSidecar) isPendingRootPathNewContainer(containerID string) bool {
	isNew, ok := s.pendingRootPathNew.Load(containerID)
	if !ok {
		return false
	}
	return isNew.(bool)
}

func (s *BkLogSidecar) clearPendingRootPathNewContainer(containerID string) {
	s.pendingRootPathNew.Delete(containerID)
}

func (s *BkLogSidecar) clearStoppedPendingRootPathNewContainers(runningContainerIDs map[string]struct{}) {
	s.pendingRootPathNew.Range(func(key, _ interface{}) bool {
		containerID := key.(string)
		if _, ok := runningContainerIDs[containerID]; !ok {
			s.pendingRootPathNew.Delete(containerID)
		}
		return true
	})
}
