#!/bin/bash
# 一键打包脚本 - 使用 PyInstaller 打包所有工具

set -e

echo "开始打包..."

# 检查是否安装了 PyInstaller
if ! command -v pyinstaller &> /dev/null; then
    echo "错误: 未找到 pyinstaller，请先安装:"
    echo "  uv sync --group dev"
    echo "  或: pip install pyinstaller"
    exit 1
fi

# 打包 kafka_consumer.py
echo "正在打包 kafka_consumer.py..."
pyinstaller --onefile --name kafka_consumer \
    --hidden-import aiokafka \
    --hidden-import kafka \
    --hidden-import kafka.protocol \
    --hidden-import kafka.client \
    --hidden-import kafka.consumer \
    --hidden-import kafka.producer \
    kafka_consumer.py

echo ""
echo "打包完成！"
echo "可执行文件位于 dist/ 目录:"
ls -lh dist/

echo ""
echo "使用方法:"
echo "  ./dist/kafka_consumer --topic <TOPIC> --group-id <GROUP_ID>"

