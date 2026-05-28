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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/define"
)

const (
	defaultContainerEventQueueSize = 100
	defaultRootPathRetryInterval   = time.Minute
)

type containerEvent struct {
	*define.ContainerEvent
	isNewContainer bool
	rootPathRetry  bool
}

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

func newContainerEvent(containerID string, isNewContainer, rootPathRetry bool) *containerEvent {
	return &containerEvent{
		ContainerEvent: &define.ContainerEvent{
			Type:        define.ContainerEventCreate,
			ContainerID: containerID,
		},
		isNewContainer: isNewContainer,
		rootPathRetry:  rootPathRetry,
	}
}

func (s *BkLogSidecar) dispatchContainerEvents() {
	for {
		select {
		case event := <-s.containerEventQueue:
			s.eventHandler(event)
		case <-s.stopCh:
			s.log.Info("stop dispatchContainerEvents")
			return
		}
	}
}

func (s *BkLogSidecar) enqueueContainerEvent(event *containerEvent) {
	if event == nil || event.ContainerEvent == nil {
		return
	}
	s.containerEventQueue <- event
}

func (s *BkLogSidecar) periodRetryRootPathEvents() {
	ticker := time.NewTicker(s.rootPathRetryDuration())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.requeuePendingRootPathEvents()
		case <-s.stopCh:
			s.log.Info("stop periodRetryRootPathEvents")
			return
		}
	}
}

func (s *BkLogSidecar) requeuePendingRootPathEvents() {
	if !s.hasPendingRootPathEvents() {
		return
	}

	containers, err := s.allContainers()
	if err != nil {
		s.log.Error(err, "list containers before root path retry failed")
		return
	}

	runningContainerIDs := make(map[string]struct{}, len(containers))
	for _, simpleContainer := range containers {
		runningContainerIDs[simpleContainer.ID] = struct{}{}
	}

	s.pendingRootPathEvents.Range(func(key, value interface{}) bool {
		containerID := key.(string)
		if _, ok := runningContainerIDs[containerID]; !ok {
			s.pendingRootPathEvents.Delete(containerID)
			return true
		}

		event, ok := value.(*containerEvent)
		if !ok || event == nil || event.ContainerEvent == nil {
			s.pendingRootPathEvents.Delete(containerID)
			return true
		}
		s.enqueueContainerEvent(newContainerEvent(event.ContainerID, event.isNewContainer, true))
		return true
	})
}

func (s *BkLogSidecar) storePendingRootPathEvent(event *containerEvent) {
	if event == nil || event.ContainerEvent == nil {
		return
	}
	if event.Type != define.ContainerEventCreate {
		return
	}
	s.pendingRootPathEvents.Store(event.ContainerID, newContainerEvent(event.ContainerID, event.isNewContainer, false))
}

func (s *BkLogSidecar) hasPendingRootPathEvent(containerID string) bool {
	_, ok := s.pendingRootPathEvents.Load(containerID)
	return ok
}

func (s *BkLogSidecar) hasPendingRootPathEvents() bool {
	hasPending := false
	s.pendingRootPathEvents.Range(func(_, _ interface{}) bool {
		hasPending = true
		return false
	})
	return hasPending
}

func (s *BkLogSidecar) clearPendingRootPathEvent(containerID string) {
	s.pendingRootPathEvents.Delete(containerID)
}

func (s *BkLogSidecar) rootPathRetryDuration() time.Duration {
	if s.rootPathRetryInterval > 0 {
		return s.rootPathRetryInterval
	}
	return defaultRootPathRetryInterval
}
