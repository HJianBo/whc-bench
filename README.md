# HTTP 压力测试工具

用于 IoT 平台的 HTTP 压力测试工具，支持从 CSV 文件读取 deviceId 并并发发送 HTTP POST 请求。

## 功能特性

- ✅ 支持从 CSV 文件读取 deviceId（支持变量）
- ✅ 异步并发请求，高性能
- ✅ 可配置并发数、总请求数、超时时间
- ✅ 详细的统计信息和错误报告

## 安装依赖

```bash
uv sync
```

## 使用方法

### 1. 生成包含 1000 个 deviceId 的 CSV 文件

```bash
uv run python generate_devices.py
```

这将生成 `devices.csv` 文件，包含 Gateway_010 到 Gateway_1009 共 1000 个 deviceId。

### 2. 运行压力测试

基本用法（必须提供 --url 和 --csv 参数）：

```bash
uv run python main.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv
```

自定义参数：

```bash
# 指定并发数为 50，总请求数为 100
uv run python main.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -c 50 -n 100

# 指定自定义 URL 和 CSV 文件
uv run python main.py --url http://example.com/api --csv my_devices.csv

# 设置超时时间为 60 秒
uv run python main.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -t 60
```

### 命令行参数

- `--url`: 目标 URL（必需）
- `--csv`: CSV 文件路径（必需）
- `-c, --concurrent`: 并发数（默认: 10）
- `-n, --requests`: 总请求数（默认: 使用 CSV 中的所有 deviceId）
- `-t, --timeout`: 请求超时时间，单位秒（默认: 30）

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

```bash
# 生成 1000 个 deviceId
uv run python generate_devices.py

# 使用 20 个并发，测试所有 1000 个设备
uv run python main.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -c 20

# 使用 50 个并发，只测试前 100 个设备
uv run python main.py --url http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command --csv devices.csv -c 50 -n 100
```

## 输出示例

```
已加载 1000 个 deviceId

开始压力测试:
  URL: http://172.23.1.80:48080/iot-platform-nw-devicedata/rpc-api/devicedata/device/command
  并发数: 20
  总请求数: 1000
  超时时间: 30秒

============================================================
压力测试结果
============================================================
总请求数: 1000
成功: 995
失败: 5
总耗时: 12.34 秒
QPS: 81.04
成功率: 99.50%
============================================================
```

