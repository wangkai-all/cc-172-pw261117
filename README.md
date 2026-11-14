# 串口采样网关（网页接入 · 本地 TCP 模拟）

串口网关采样数据的网页接入系统。网关设备由**本地 TCP 模拟器**扮演，无需插硬件；
网关服务通过 TCP 接入字节流，按 [doc/protocol.md](doc/protocol.md) 的字节格式解包，
网页实时展示接入过程。纯 Python 标准库实现，零第三方依赖（Python ≥ 3.10）。

## 快速开始

```bash
# 1. 启动模拟器（数据端口 9100，控制端口 9101）
python3 -m gateway.simulator --port 9100 --control 9101 --interval 0.2

# 2. 另开终端，启动网关服务（网页 8080）
python3 -m gateway.app --http-port 8080 --db data/gateway.db

# 3. 打开网页
#    http://127.0.0.1:8080
```

网页上可以：实时查看接入样本（原始字节 + 解析值 + 配置版本）、修改各通道
倍率/单位（带生效序号）、查看坏包与噪声的字节流偏移、暂停/恢复接入、
触发模拟器的各种现场情况、导出 CSV/JSON 明细。

## 功能与验证点

| 需求 | 实现 |
| --- | --- |
| 起止标记/通道/序号/原始值/校验码解包 | `gateway/parser.py` 增量分帧，格式见 `doc/protocol.md` |
| 分片到达 | 缓冲凑不满 12 字节持续等待；模拟器 `fragment` 触发 |
| 连续粘包 | 一次读取内连续解出多帧；模拟器 `sticky` 触发 |
| 噪声字节 | 起始标记前的字节记为噪声段（含流偏移）；模拟器 `noise` 触发 |
| 坏包隔离 | 校验码/结束标记错误单独记录原始字节与偏移，不影响后续帧；模拟器 `bad` 触发 |
| 倍率/单位页面可改，配置带生效序号 | 配置版本表 `(生效 epoch, 生效序号)`，按样本序号字典序解析 |
| 同一段缓冲数据不因材处理时间套不同配置 | 配置适用性只依赖 `(channel, epoch, seq)`，与处理时刻无关；暂停积压后恢复可复现验证 |
| 保存原始字节/解析值/配置版本 | `samples` 表：`raw_hex`、`raw_value`、`scaled`、`config_version` |
| 重复采样不重复入库 | `(channel, epoch, seq)` 唯一约束 + 分类前判重 |
| 序号回绕与设备重启分别识别 | 回绕：序号贴近 65535→0；重启：连接断开后序号回跳（重连佐证），两者各开新 epoch 并分别记事件 |
| 实时接入/切换前后数值/错误位置 | SSE 推送；样本表按配置版本着色区分切换边界；错误表含流偏移 |
| 暂停/恢复/导出 | 暂停时字节积压不解析；`/api/export.csv`、`/api/export.json` |
| 状态重启后保留 | SQLite 持久化样本、配置版本、通道游标、字节流偏移 |
| 样例验证切换边界 | `tests/test_pipeline.py::TestConfigBoundary`、`tests/test_integration.py::test_02` |

## 网页 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 状态快照（连接/暂停/统计/通道游标/配置/最近样本/错误/事件） |
| GET | `/api/stream` | SSE 实时推送：`sample` `error` `event` `config` `state` `stats` |
| POST | `/api/config` | 下发配置 `{channel, scale, unit, effective_seq, effective_epoch?}` |
| POST | `/api/control` | `{action: "pause"\|"resume"}` 暂停/恢复接入 |
| POST | `/api/simulate` | `{cmd}` 转发模拟器触发命令 |
| GET | `/api/export.csv` | 样本明细 CSV（含原始字节 hex、配置版本、流偏移） |
| GET | `/api/export.json` | 全量明细 JSON（样本/错误/配置/事件/通道状态） |

## 模拟器控制命令

网页按钮或 `echo <cmd> | nc 127.0.0.1 9101`：

`fragment`（分片）· `sticky`（粘包）· `noise`（噪声）· `bad`（坏包）·
`dup`（重复采样）· `wrap`（序号回绕）· `restart`（设备重启：序号归零并断连）· `status`

## 关键设计

- **配置按序号生效**：`resolve_config(channel, epoch, seq)` 取「生效点 ≤ 样本点」的
  最新配置版本。样本在缓冲里压了多久、何时被处理，都不影响套用的配置——
  暂停 → 下发配置 → 恢复，积压数据仍按各自序号正确分到切换前后两侧。
- **epoch（代数）**：序号回绕或设备重启时 epoch+1，样本以 `(channel, epoch, seq)`
  唯一。回绕（贴近边界 65535→0）与重启（连接断开后序号回跳）分别识别、分别记事件；
  连接未断时的序号回跳且该序号当前 epoch 已入库 → 判重发，不重复入库。
- **坏包隔离**：候选 12 字节校验失败即记录（类型/偏移/原始字节），随后前进 1 字节
  重扫（允许起始标记重叠）；坏包覆盖的字节不再重复计为噪声。
- **持久化**：SQLite（WAL）。进程重启后通道游标、配置版本、字节流偏移从库中恢复，
  去重与回绕/重启判定连续不断档。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

27 个用例：分帧器（分片/粘包/噪声/坏包/截断/偏移）、序号分类、配置边界
（含跨 epoch）、去重、重开库状态保留，以及端到端（实时接入、暂停积压下的
配置边界、触发器、回绕与重启、网关重启续传、导出）。

## 目录

```
doc/protocol.md      报文字节格式与解包规则
gateway/protocol.py  组帧/解帧/校验码
gateway/parser.py    增量分帧（分片/粘包/噪声/坏包隔离）
gateway/pipeline.py  回绕与重启识别、配置解析、去重
gateway/store.py     SQLite 持久化
gateway/app.py       TCP 接入 + HTTP/SSE + 暂停恢复 + 导出
gateway/simulator.py 本地 TCP 模拟器（含控制端口）
static/              网页（实时样本/配置/错误/事件）
tests/               单元 + 端到端测试
```
