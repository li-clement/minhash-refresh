#!/usr/bin/env bash
# 启动/重启 node1 上的 Ray head（systemd user 服务，已 enable，开机会自启）
set -euo pipefail
systemctl --user restart ray-head.service
sleep 10
systemctl --user is-active ray-head.service
/home/lee/code/raytest/.venv/bin/ray status
