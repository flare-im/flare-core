#!/usr/bin/env python3
"""性能回归门禁：跑 benches/perf_baseline.rs，判它的结果还合不合理。

背景：这个 bench 一直存在，但**从来没有任何 CI 跑过它**。`verify.sh` 里的
`cargo check --benches` 只证明它能编过——编得过和跑得出正确数字是两回事，
而 verify.sh 本身也只在有人手动执行时才跑。等于基线建了、没人看。

为什么不按绝对耗时设阈值：
    GitHub 的共享 runner 上同一段代码的耗时能差好几倍（邻居负载、CPU 型号、
    是否降频都不受控）。按本机数字设阈值，结果必然是长期红——而这个项目
    已经吃过一次「非阻塞 + 长期红 = 告警彻底失效」的亏，不再犯第二次。

所以判据分三类，全都与机器速度无关或留足量级余量：

  1. **齐全性**：点名的 benchmark 一个都不能少。少了就是判据被悄悄缩水
     （改名、删掉、条件编译掉），这类「什么都没验还输出绿」比红更危险。
  2. **同轮比值**：同一次运行内两项之间的关系。机器快慢会同时作用于分子
     分母，比值因此比绝对值稳定得多，能抓住算法级回退。但「稳定」不等于
     「精确」——两项耗时接近时排序照样会翻转，所以下限要按实测的波动幅度
     来定（见 RATIOS 里的注释），不能想当然按 1.0。
  3. **量级上限**：留 50~100 倍余量的绝对天花板。它抓不住 20% 的劣化，
     但能抓住「热路径里混进了阻塞调用 / 退化成 O(n²)」这种数量级事故。

用法：
    python3 scripts/check_perf_baseline.py            # 自己跑 bench 再判
    python3 scripts/check_perf_baseline.py results.json   # 判已有结果
"""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 点名清单。bench 里加了新项不必登记，但**这些少一个就判红**。
REQUIRED = [
    "codec.protobuf.round_trip.256b",
    "codec.json.round_trip.256b",
    "codec.protobuf_gzip.round_trip.1kb",
    "pipeline.process_raw.validate_no_response.256b",
    "connection_manager.add_update_remove",
    "connection_manager.broadcast.1000x256b",
    "connection_manager.broadcast_frame_explicit_parser.1000x256b",
    "connection_manager.cleanup_timeout_trait.1000",
]

# (慢项, 快项, 最小倍数, 说明)：同一轮内的比值，与机器速度无关。
RATIOS = [
    (
        "codec.json.round_trip.256b",
        "codec.protobuf.round_trip.256b",
        2.0,
        "protobuf 编解码该显著快于 JSON（实测约 6 倍）。跌到 2 倍以内，"
        "多半是 protobuf 路径上混进了字符串化/反射之类的绕路。",
    ),
]

# 试过、又刻意撤掉的一条比值（记在这里，免得以后有人凭直觉再加回来）：
#
#   connection_manager.broadcast.1000x256b
#     ÷ connection_manager.broadcast_frame_explicit_parser.1000x256b
#
# 直觉上「序列化一次再广播」该稳赢「每条连接各序列化一次」。但同一台机器
# 连跑三轮，比值是 1.20 / 0.98 / 0.77——排序反复翻转。也就是说在 1000 条
# 内存连接这个规模上，两条路的差异整个落在噪声里，那个优化没有可测收益。
# 按任何下限设卡都只会得到一个随机红的门禁。
# 这两项各自的绝对天花板（见 CEILINGS）已经能兜住数量级事故，够了。

# 量级天花板（ns/op），按实测值留 50~100 倍余量。抓的是数量级事故，不是抖动。
CEILINGS = {
    "codec.protobuf.round_trip.256b": 100_000,  # 实测 ~842
    "codec.json.round_trip.256b": 500_000,  # 实测 ~5,031
    "pipeline.process_raw.validate_no_response.256b": 100_000,  # 实测 ~698
    "connection_manager.add_update_remove": 100_000,  # 实测 ~923
    "connection_manager.broadcast.1000x256b": 50_000_000,  # 实测 ~737,206
    "connection_manager.broadcast_frame_explicit_parser.1000x256b": 50_000_000,  # 实测 ~616,000
}


def run_bench() -> list:
    """跑 bench 并取出它 stdout 里的 JSON。

    cargo 会在 JSON 前后混入编译进度等噪声，所以从第一个 '[' 开始截。
    """
    proc = subprocess.run(
        [
            "cargo",
            "bench",
            "--bench",
            "perf_baseline",
            "--features",
            "server,compression-gzip",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print("✗ bench 没跑起来（这本身就是回归：基线不可执行）", file=sys.stderr)
        print(proc.stdout[-3000:], file=sys.stderr)
        print(proc.stderr[-3000:], file=sys.stderr)
        sys.exit(1)

    start = proc.stdout.find("[")
    end = proc.stdout.rfind("]")
    if start < 0 or end < 0:
        print("✗ bench 跑完了但没输出 JSON —— 它的输出格式变了？", file=sys.stderr)
        print(proc.stdout[-3000:], file=sys.stderr)
        sys.exit(1)
    return json.loads(proc.stdout[start : end + 1])


def main() -> int:
    if len(sys.argv) > 1:
        results = json.loads(Path(sys.argv[1]).read_text())
    else:
        results = run_bench()

    by_name = {r["name"]: r for r in results}
    problems = []

    # 先把数字打出来：判红时 stdout/stderr 交错，表格放后面会被冲散。
    print("性能基线（本轮实测）：")
    for r in results:
        print(f"  {r['name']:<58} {r['ns_per_op']:>14,.0f} ns/op")
    print()

    missing = [n for n in REQUIRED if n not in by_name]
    if missing:
        problems.append(
            "  ✗ 少了这些 benchmark：\n"
            + "\n".join(f"      {n}" for n in missing)
            + "\n    改名或删项都要同步改本文件的 REQUIRED，"
            "否则门禁会在「什么都没验」的状态下输出绿。"
        )

    for slow, fast, min_ratio, why in RATIOS:
        if slow not in by_name or fast not in by_name:
            continue  # 齐全性那条已经报过了
        ratio = by_name[slow]["ns_per_op"] / by_name[fast]["ns_per_op"]
        if ratio < min_ratio:
            problems.append(
                f"  ✗ {slow} / {fast} = {ratio:.2f}，低于下限 {min_ratio}\n"
                f"    {why}"
            )

    for name, ceiling in CEILINGS.items():
        if name not in by_name:
            continue
        actual = by_name[name]["ns_per_op"]
        if actual > ceiling:
            problems.append(
                f"  ✗ {name}: {actual:,.0f} ns/op 超过量级上限 {ceiling:,} ns/op\n"
                f"    上限留了 50~100 倍余量，撞上它说明是数量级事故"
                f"（热路径混进阻塞调用、复杂度退化），不是 runner 抖动。"
            )

    if problems:
        print("性能回归：", file=sys.stderr)
        print("\n".join(problems), file=sys.stderr)
        return 1

    print(f"  ✓ {len(REQUIRED)} 项齐全，{len(RATIOS)} 条比值与 {len(CEILINGS)} 条量级上限均通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
