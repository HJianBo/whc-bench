# WHC Bench - IoT 平台压力测试工具

用于 IoT 平台的 HTTP 压力测试工具，支持从 CSV 文件读取 deviceId 并并发发送请求。

## 功能特性

### HTTP 压力测试 (`http_stress.py`)
- ✅ 支持从 CSV 文件读取 deviceId（支持变量）
- ✅ 异步并发请求，高性能
- ✅ 可配置并发数、总请求数、超时时间
- ✅ 详细的统计信息和错误报告

## 工程结构

```
whc-bench/
├── main.py              # HTTP 压力测试入口（向后兼容）
├── http_stress.py       # HTTP 压力测试实现
├── utils.py             # 共享工具函数（CSV 读取等）
├── devices.csv          # 设备 ID 列表
├── pyproject.toml       # 项目配置和依赖
└── README.md           # 本文档
```

## 安装依赖

```bash
uv sync
```

## 使用方法

### 1. HTTP 压力测试

#### 使用 `main.py`（向后兼容）

```bash
uv run python main.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv
```

#### 使用 `http_stress.py`

```bash
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv
```

#### 自定义参数

```bash
# 指定并发数为 50，每个设备循环 100 次
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -C 50 -n 100

# 只测试前 100 个设备
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -c 100

# 设置请求间隔为 100ms
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -i 100

# 设置超时时间为 60 秒
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -t 60
```

### 命令行参数

#### HTTP 压力测试参数

- `--url`: 目标 URL（必需）
- `--csv`: CSV 文件路径（必需）
- `-C, --concurrent`: 并发数（默认: 10）
- `-c, --device-count`: DeviceId 数量，读取 CSV 文件的前 N 个 ID（默认: 使用所有）
- `-n, --loop-count`: 每个 DeviceId 的循环次数（默认: 10）
- `-i, --interval`: 每个循环发送的间隔（毫秒）（默认: 0）
- `-t, --timeout`: 请求超时时间（秒）（默认: 30）

### CSV 文件格式

CSV 文件应包含 `deviceId` 列，例如：

```csv
deviceId
Gateway_010
Gateway_011
Gateway_012
...
```

如果没有 `deviceId` 列，程序会使用第一列作为 deviceId。

## 示例

### HTTP 压力测试示例

```bash
# 使用 20 个并发，测试所有设备
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -C 20

# 使用 50 个并发，只测试前 100 个设备，每个设备循环 50 次
uv run python http_stress.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -C 50 -c 100 -n 50
```

## 输出示例

### HTTP 压力测试输出

```
已加载 1000 个 deviceId

开始压力测试:
  URL: http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command
  并发数: 20
  DeviceId 数量: 1000
  每个 DeviceId 循环次数: 10
  总请求数: 10000
  超时时间: 30秒

============================================================
压力测试结果
============================================================
总请求数: 10000
成功: 9950
失败: 50
总耗时: 12.34 秒
QPS: 810.40
成功率: 99.50%
============================================================
```
