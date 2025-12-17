# WHC Bench - IoT 平台测试工具

用于 IoT 平台的测试工具集，支持 Kafka 消费者和 MQTT 客户端测试。

## 功能特性

### Kafka 消费者 (`kafka_consumer.py`)
- ✅ 支持 Kafka 消息消费
- ✅ 可配置消费者组和主题
- ✅ 详细的消息统计和监控

## 工程结构

```
whc-bench/
├── kafka_consumer.py    # Kafka 消费者实现
├── utils.py             # 共享工具函数
├── devices.csv          # 设备 ID 列表
├── pyproject.toml       # 项目配置和依赖
└── README.md           # 本文档
```

## 安装依赖

```bash
uv sync
```

## 使用方法

### Kafka 消费者

```bash
uv run python kafka_consumer.py --bootstrap-servers localhost:9092 --topic test-topic --group-id test-group
```

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
