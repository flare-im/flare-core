#!/bin/bash
# 内存安全闸:大连接压测必挂。每 12s 查系统可用内存,低于 FLOOR 立即杀 loadconns 释放连接,
# 杜绝把机器压入 swap thrash(曾因无防护冲 6 万连接导致 SSH/HTTP 30 分钟无响应)。
# 用法: FLOOR=2800 DUR=900 safemon.sh   (FLOOR 单位 MB;DUR 监控时长秒)
# 注意 pkill -f 自匹配:用括号 [l]oadconns,且本脚本内不出现该 pattern 明文(避免误杀自身)。
set -u
FLOOR=${FLOOR:-2800}; DUR=${DUR:-900}
END=$(( $(date +%s) + DUR ))
gwpid=$(docker inspect svc-signaling-gateway-1 --format '{{.State.Pid}}' 2>/dev/null)
while [ "$(date +%s)" -lt "$END" ]; do
  av=$(free -m | awk '/^Mem/{print $7}')
  fd=$(ls /proc/"$gwpid"/fd 2>/dev/null | wc -l)
  gm=$(docker stats --no-stream --format '{{.MemUsage}}' svc-signaling-gateway-1 2>/dev/null)
  lc=$(pgrep -cf "/root/[l]oadconns")
  echo "[$(date +%T)] avail=${av}MB gw_fd=$fd gw_mem=$gm loadconns=$lc"
  if [ "${av:-0}" -lt "$FLOOR" ]; then
    echo "!!! SAFETY: available ${av}MB < ${FLOOR}MB, killing load client"
    pkill -9 -f "/root/[l]oadconns"; break
  fi
  [ "$lc" = "0" ] && { echo "load client exited"; break; }
  sleep 12
done
echo "safemon done"
