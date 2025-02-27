#!/bin/bash
# Tencent is pleased to support the open source community by making
# 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
# Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
# Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://opensource.org/licenses/MIT
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.


write_root_conf() {
  path="$1"
  system="$2"
  arch="$3"
  cat <<EOF > "$path"
EOF
  cat <<EOF >> "$path"
# ================================ Outputs =====================================
EOF
  if [ "$system" = "windows" ]; then
    cat <<EOF >> "$path"
max_procs: 1
output.gse:
  endpoint: 127.0.0.1:47000
path.data: 'C:\gse\data'
path.logs: 'C:\gse\logs'
path.pid: 'C:\gse\logs'
EOF
  else
    cat <<EOF >> "$path"
output.bkpipe:
  endpoint: /var/run/ipc.state.report
  synccfg: true
path.pid: /var/run/gse
path.logs: /var/log/gse
path.data: /var/lib/gse
EOF
  fi
  cat <<EOF >> "$path"
seccomp.enabled: false


# ================================ Logging ======================================
# Available log levels are: critical, error, warning, info, debug
logging.level: error
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
logging.path: 'C:\gse\logs'
EOF
  else
    cat <<EOF >> "$path"
logging.path: /var/log/gse
EOF
  fi
  cat <<EOF >> "$path"
logging.maxsize: 10
logging.maxage: 3
logging.backups: 5


EOF
  if [ "$system" != "aix" ]; then
  cat <<EOF >> "$path"
# ============================= Resource ==================================
#resource_limit:
#  # CPU 资源限制 单位 core(float64)
#  cpu: 1
#  # 内存资源限制 单位 MB(int)
#  mem: 512


EOF
  fi
  cat <<EOF >> "$path"
# ================================= Tasks =======================================
bkmonitorbeat:
  node_id: 0
  ip: 127.0.0.1
  bk_cloud_id: 0
  # 主机CMDB信息hostid文件路径
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
  host_id_path: 'C:\gse\data\host\hostid'
EOF
  else
    cat <<EOF >> "$path"
  host_id_path: /var/lib/gse/host/hostid
EOF
  fi
  cat <<EOF >> "$path"
  # 子任务目录
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
  include: 'C:\gse\plugins\etc\bkmonitorbeat'
EOF
  else
    cat <<EOF >> "$path"
  include: /usr/local/gse/plugins/etc/bkmonitorbeat/
EOF
  fi
  cat <<EOF >> "$path"
  # 当前节点所属业务
  bk_biz_id: 1
  # stop/reload旧任务清理超时
  clean_up_timeout: 1s
  # 事件管道缓冲大小
  event_buffer_size: 10
  # 启动模式：daemon（正常模式）,check（执行一次，测试用）
  mode: daemon
  disable_netlink: false
  metrics_batch_size: 1024
  # 管理服务，包含指标和调试接口, 可动态reload开关或变更监听地址（unix使用SIGUSR2,windows发送bkreload2）
  # admin_addr: localhost:56060
  # ================================ BK Metrics Pusher =================================================
  bk_metrics_pusher:
    disabled: false
    dataid: 1100014
    period: 60s
    batch_size: 1024
    labels: []
    metric_relabel_configs:
      - source_labels:
          - __name__
        regex: ^(go_|process_|bkmonitorbeat_global_|bkmonitorbeat_metricbeat_metrics_sent_total|bkmonitorbeat_metricbeat_process_duration_seconds).*
        action: keep

  # 心跳采集配置
  heart_beat:
    global_dataid: 1100001
    child_dataid: 1100002
    period: 60s
    publish_immediately: true

EOF
  cat <<EOF >> "$path"
  # 静态资源采集配置
EOF
  if [ "$system" = "aix" ]; then
    cat <<EOF >> "$path"
#  static_task:
#    dataid: 1100010
#    tasks:
#    - task_id: 100
#      period: 1m
#      check_period: 1m
#      report_period: 6h

EOF
  else
    cat <<EOF >> "$path"
  static_task:
    dataid: 1100010
    tasks:
    - task_id: 100
      period: 1m
      check_period: 1m
      report_period: 6h

EOF
  fi
  cat <<EOF >> "$path"
  # 主机性能数据采集
EOF
  if [ "$system" = "aix" ]; then
    cat <<EOF >> "$path"
#  basereport_task:
#    task_id: 101
#    dataid: 1001
#    period: 1m
#    cpu:
#      stat_times: 4
#      info_period: 1m
#      info_timeout: 30s
#    disk:
#      stat_times: 1
#      mountpoint_black_list: ["docker","container","k8s","kubelet"]
#      fs_type_white_list: ["overlay","btrfs","ext2","ext3","ext4","reiser","xfs","ffs","ufs","jfs","jfs2","vxfs","hfs","apfs","refs","ntfs","fat32","zfs"]
#      collect_all_device: true
#    mem:
#      info_times: 1
#    net:
#      stat_times: 4
#      revert_protect_number: 100
#      skip_virtual_interface: false
#      interface_black_list: ["veth", "cni", "docker", "flannel", "tunnat", "cbr", "kube-ipvs", "dummy"]
#      force_report_list: ["bond"]

EOF
  else
    cat <<EOF >> "$path"
  basereport_task:
    task_id: 101
    dataid: 1001
    period: 1m
    cpu:
      stat_times: 4
      info_period: 1m
      info_timeout: 30s
    disk:
      stat_times: 1
      mountpoint_black_list: ["docker","container","k8s","kubelet"]
      fs_type_white_list: ["overlay","btrfs","ext2","ext3","ext4","reiser","xfs","ffs","ufs","jfs","jfs2","vxfs","hfs","apfs","refs","ntfs","fat32","zfs"]
      collect_all_device: true
    mem:
      info_times: 1
    net:
      stat_times: 4
      revert_protect_number: 100
      skip_virtual_interface: false
      interface_black_list: ["veth", "cni", "docker", "flannel", "tunnat", "cbr", "kube-ipvs", "dummy"]
      force_report_list: ["bond"]

EOF
  fi
  cat <<EOF >> "$path"
  # 主机异常事件采集（磁盘满、磁盘只读、Corefile 事件以及 OOM 事件）
EOF
  if [ "$system" = "aix" ]; then
    cat <<EOF >> "$path"
#  exceptionbeat_task:
#    task_id: 102
#    dataid: 1000
#    period: 1m
#    check_bit: "C_DISK_SPACE|C_DISKRO|C_CORE|C_OOM"
#    check_disk_ro_interval: 60
#    check_disk_space_interval: 60
#    check_oom_interval: 10
#    used_max_disk_space_percent: 95

EOF
  else
    cat <<EOF >> "$path"
  exceptionbeat_task:
    task_id: 102
    dataid: 1000
    period: 1m
    check_bit: "C_DISK_SPACE|C_DISKRO|C_CORE|C_OOM"
    check_disk_ro_interval: 60
    check_disk_space_interval: 60
    check_oom_interval: 10
    used_max_disk_space_percent: 95

  # 进程采集：同步 CMDB 进程配置文件到 bkmonitorbeat 子任务文件夹下
  procconf_task:
    task_id: 103
    period: 1m
    perfdataid: 1007
    portdataid: 1013
    converge_pid: true
    disable_netlink: false
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
    hostfilepath: 'C:\gse\data\host\hostid'
    dst_dir: 'C:\gse\plugins\etc\bkmonitorbeat'
EOF
  else
    cat <<EOF >> "$path"
    hostfilepath: /var/lib/gse/host/hostid
    dst_dir: /usr/local/gse/plugins/etc/bkmonitorbeat
EOF
  fi
  cat <<EOF >> "$path"

EOF
    cat <<EOF >> "$path"
  # 进程采集：同步自定义进程配置文件到 bkmonitorbeat 子任务文件夹下
#  procsync_task:
#    task_id: 104
#    period: 1m
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
#    dst_dir: 'C:\gse\plugins\etc\bkmonitorbeat'
EOF
  else
    cat <<EOF >> "$path"
#    dst_dir: /usr/local/gse/plugins/etc/bkmonitorbeat
EOF
  fi
  cat <<EOF >> "$path"

EOF
fi
  cat <<EOF >> "$path"
  #### tcp_task child config #####
  # tcp任务全局设置
  #  tcp_task:
  #    dataid: 101176
  #    # 缓冲区最大空间
  #    max_buffer_size: 10240
  #    # 最大超时时间
  #    max_timeout: 30s
  #    # 最小检测间隔
  #    min_period: 3s
  #    # 任务列表
  #    tasks:
  #      - task_id: 1
  #        bk_biz_id: 1
  #        period: 60s
  #        # 检测超时（connect+read总共时间）
  #        timeout: 3s
  #        target_host: 127.0.0.1
  #        target_port: 9202
  #        available_duration: 3s
  #        # 请求内容
  #        request: hi
  #        # 请求格式（raw/hex）
  #        request_format: raw
  #        # 返回内容
  #        response: hi
  #        # 内容匹配方式
  #        response_format: eq

  #### udp_task child config #####
  #  udp_task:
  #    dataid: 0
  #    # 缓冲区最大空间
  #    max_buffer_size: 10240
  #    # 最大超时时间
  #    max_timeout: 30s
  #    # 最小检测间隔
  #    min_period: 3s
  #    # 最大重试次数
  #    max_times: 3
  #    # 任务列表
  #    tasks:
  #      - task_id: 5
  #        bk_biz_id: 1
  #        times: 3
  #        period: 60s
  #        # 检测超时（connect+read总共时间）
  #        timeout: 3s
  #        target_host: 127.0.0.1
  #        target_port: 9201
  #        available_duration: 3s
  #        # 请求内容
  #        request: hello
  #        # 请求格式（raw/hex）
  #        request_format: raw
  #        # 返回内容
  #        response: hello
  #        # 内容匹配方式
  #        response_format: eq
  #        # response为空时是否等待返回
  #        wait_empty_response: false

  #### http_task child config #####
  #  http_task:
  #    dataid: 0
  #    # 缓冲区最大空间
  #    max_buffer_size: 10240
  #    # 最大超时时间
  #    max_timeout: 30s
  #    # 最小检测间隔
  #    min_period: 3s
  #    # 任务列表
  #    tasks:
  #      - task_id: 5
  #        bk_biz_id: 1
  #        period: 60s
  #        # proxy: http://proxy.qq.com:8000
  #        # 是否校验证书
  #        insecure_skip_verify: false
  #        disable_keep_alives: false
  #        # 检测超时（connect+read总共时间）
  #        timeout: 3s
  #        # 采集步骤
  #        steps:
  #          - url: http://127.0.0.1:9203/path/to/test
  #            method: GET
  #            headers:
  #              referer: http://bk.tencent.com
  #            available_duration: 3s
  #            request: ""
  #            # 请求格式（raw/hex）
  #            request_format: raw
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
  #            response: "response"
EOF
  else
    cat <<EOF >> "$path"
  #            response: "/path/to/test"
EOF
  fi
  cat <<EOF >> "$path"
  #            # 内容匹配方式
  #            response_format: eq
  #            response_code: 200,201

  #### metricbeat_task child config #####
  #  metricbeat_task:
  #    dataid: 0
  #    # 缓冲区最大空间
  #    max_buffer_size: 10240
  #    # 最大超时时间
  #    max_timeout: 100s
  #    # 最小检测间隔
  #    min_period: 3s
  #    tasks:
  #      - task_id: 5
  #        bk_biz_id: 1
  #        # 周期
  #        period: 60s
  #        # 超时
  #        timeout: 60s
  #        module:
  #          module: mysql
  #          metricsets: ["allstatus"]
  #          enabled: true
  #          hosts: ["root:mysql123@tcp(127.0.0.1:3306)/"]

  #### script_task child config #####
  #  script_task:
  #    dataid: 0
  #    tasks:
  #      - bk_biz_id: 2
  #        command: echo 'value' 45
  #        dataid: 0
  #        period: 1m
  #        task_id: 7
  #        timeout: 60s
  #        user_env: {}

  #### keyword_task child config #####
  #  keyword_task:
  #    dataid: 0
  #    tasks:
  #      - task_id: 5
  #        bk_biz_id: 2
  #        dataid: 12345
  #        # 采集文件路径
  #        paths:
EOF
  if [ "$system" = "windows" ]; then
       cat <<EOF >> "$path"
  #          - 'logs'
EOF
  else
    cat <<EOF >> "$path"
  #          - '/var/log/messages'
EOF
  fi
  cat <<EOF >> "$path"
  #
  #        # 需要排除的文件列表，正则表示
  #        # exclude_files:
  #        #  - '.*\.py'
  #
  #        # 文件编码类型
  #        encoding: 'utf-8'
  #        # 文件未更新需要删除的超时等待
  #        close_inactive: '86400s'
  #        # 上报周期
  #        report_period: '1m'
  #        # 日志关键字匹配规则
  #        keywords:
  #          - name: HttpError
  #            pattern: '.*ERROR.*'
  #
  #        # 结果输出格式
  #        # output_format: 'event'
  #
  #        # 上报时间单位，默认ms
  #        # time_unit: 'ms'
  #
  #        # 采集目标
  #        target: '0:127.0.0.1'
  #        # 注入的labels
  #        labels:
  #          - bk_target_service_category_id: ""
  #            bk_collect_config_id: "59"
  #            bk_target_cloud_id: "0"
  #            bk_target_topo_id: "1"
  #            bk_target_ip: "127.0.0.1"
  #            bk_target_service_instance_id: ""
  #            bk_target_topo_level: "set"
EOF
}
