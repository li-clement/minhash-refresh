#!/usr/bin/env bash
set -euo pipefail
systemctl --user stop ray-head.service
echo stopped
