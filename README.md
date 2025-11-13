# WHC Bench - IoT 平台压力测试工具

用于 IoT 平台的 HTTP 和 MQTT 压力测试工具，支持从 CSV 文件读取 deviceId 并并发发送请求或连接 MQTT Broker。

## 功能特性

### HTTP 压力测试 (`http_stress.py`)
- ✅ 支持从 CSV 文件读取 deviceId（支持变量）
- ✅ 异步并发请求，高性能
- ✅ 可配置并发数、总请求数、超时时间
- ✅ 详细的统计信息和错误报告

### MQTT 客户端 (`mqtt_client.py`)
- ✅ 支持多个设备并发连接到 MQTT Broker
- ✅ 每个客户端使用 deviceId 作为 Client ID 和 Username
- ✅ 自动订阅 `/v1/devices/<deviceId>/command` 主题
- ✅ 接收消息并测量时延（解析 payload 中的 timestamp）
- ✅ 详细的统计信息（平均、最小、最大、P50/P95/P99 时延）

## 工程结构

```
whc-bench/
├── main.py              # HTTP 压力测试入口（向后兼容）
├── http_stress.py       # HTTP 压力测试实现
├── mqtt_client.py       # MQTT 客户端实现
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

### 2. MQTT 客户端

#### 基本用法

```bash
uv run python mqtt_client.py --broker 172.23.1.80 --port 1883 --csv devices.csv
```

#### 自定义参数

```bash
# 指定端口和密码
uv run python mqtt_client.py --broker 172.23.1.80 --port 1883 --csv devices.csv -p mypassword

# 只连接前 10 个设备
uv run python mqtt_client.py --broker 172.23.1.80 --port 1883 --csv devices.csv -c 10

# 设置 keepalive 时间为 120 秒
uv run python mqtt_client.py --broker 172.23.1.80 --port 1883 --csv devices.csv -k 120
```

#### MQTT 客户端说明

- **Client ID**: 使用 `<deviceId>` 作为 MQTT Client ID
- **Username**: 使用 `<deviceId>` 作为 MQTT Username
- **Password**: 默认为 `123456`，可通过 `-p` 参数自定义
- **订阅主题**: `/v1/devices/<deviceId>/command`
- **时延测量**: 解析消息 payload 中的 `timestamp` 字段，与当前时间比较计算时延

### 命令行参数

#### HTTP 压力测试参数

- `--url`: 目标 URL（必需）
- `--csv`: CSV 文件路径（必需）
- `-C, --concurrent`: 并发数（默认: 10）
- `-c, --device-count`: DeviceId 数量，读取 CSV 文件的前 N 个 ID（默认: 使用所有）
- `-n, --loop-count`: 每个 DeviceId 的循环次数（默认: 10）
- `-i, --interval`: 每个循环发送的间隔（毫秒）（默认: 0）
- `-t, --timeout`: 请求超时时间（秒）（默认: 30）

#### MQTT 客户端参数

- `--broker`: MQTT Broker 地址（必需）
- `--port`: MQTT Broker 端口（默认: 1883）
- `--csv`: CSV 文件路径（必需）
- `-c, --device-count`: DeviceId 数量，读取 CSV 文件的前 N 个 ID（默认: 使用所有）
- `-p, --password`: MQTT 密码（默认: 123456）
- `-k, --keepalive`: Keepalive 时间（秒）（默认: 60）

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

### MQTT 客户端示例

```bash
# 连接所有设备到 MQTT Broker
uv run python mqtt_client.py --broker 172.23.1.80 --port 1883 --csv devices.csv

# 只连接前 10 个设备
uv run python mqtt_client.py --broker 172.23.1.80 --port 1883 --csv devices.csv -c 10
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

### MQTT 客户端输出

```
已加载 10 个 deviceId

开始连接 MQTT Broker:
  Broker: 172.23.1.80:1883
  设备数量: 10
  密码: 123456

[Gateway_010] 已连接到 MQTT Broker 172.23.1.80:1883
[Gateway_010] 已订阅主题: /v1/devices/Gateway_010/command
[Gateway_011] 已连接到 MQTT Broker 172.23.1.80:1883
[Gateway_011] 已订阅主题: /v1/devices/Gateway_011/command
...
[Gateway_010] 收到消息，时延: 45ms (消息数: 1)
[Gateway_011] 收到消息，时延: 52ms (消息数: 1)
...

（按 Ctrl+C 停止并显示统计信息）

================================================================================
MQTT 客户端统计信息
================================================================================

设备: Gateway_010
  收到消息数: 100
  错误数: 0
  平均时延: 48.50ms
  最小时延: 12.00ms
  最大时延: 156.00ms
  P50 时延: 45.00ms
  P95 时延: 98.00ms
  P99 时延: 145.00ms

...

--------------------------------------------------------------------------------
总计:
  总消息数: 1000
  总错误数: 0

全局统计:
  平均时延: 49.23ms
  最小时延: 10.00ms
  最大时延: 200.00ms
  P50 时延: 47.00ms
  P95 时延: 102.00ms
  P99 时延: 158.00ms
================================================================================
```

## 注意事项

1. **MQTT 客户端**: 程序会持续运行并接收消息，按 `Ctrl+C` 停止并显示统计信息
2. **时延测量**: MQTT 客户端会解析消息 payload 中的 `timestamp` 字段（支持多种格式），如果找不到 timestamp，会记录错误但不会中断程序
3. **并发连接**: MQTT 客户端会为每个设备创建一个独立的连接，请确保 MQTT Broker 支持足够的并发连接数
