#!/bin/bash
# 批量签发认证 token(走 api-gateway 的服务端签发端点,与 web app SDK 托管流程一致)。
# 用法: mint_tokens.sh COUNT OUTFILE [START] [ENDPOINT]
#   COUNT    要签的用户数(u00000001.. 递增)
#   OUTFILE  输出文件,每行 "<userId> <token>"(供 loadconns 读取)
#   START    起始编号(默认 1)
#   ENDPOINT token 签发地址(默认 http://127.0.0.1:50060/api/v1/auth/tokens)
# 说明: ttlSecs 取 9000(2.5h)覆盖压测时长;-P 60 并行,~1.9s/500 个。
set -u
COUNT=${1:-500}; OUT=${2:-/tmp/tokens.txt}; START=${3:-1}
ENDPOINT=${4:-http://127.0.0.1:50060/api/v1/auth/tokens}
> "$OUT"
seq "$START" $((START + COUNT - 1)) | xargs -P 60 -I{} bash -c '
  uid=$(printf "u%08d" {})
  tok=$(curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
    -d "{\"userId\":\"$uid\",\"tenantId\":\"0\",\"deviceId\":\"load\",\"ttlSecs\":9000}" \
    "'"$ENDPOINT"'" 2>/dev/null | grep -oE "\"token\":\"[^\"]+\"" | cut -d\" -f4)
  [ -n "$tok" ] && echo "$uid $tok" >> '"$OUT"'
'
echo "签发完成: $(wc -l < "$OUT") 个 token -> $OUT"
