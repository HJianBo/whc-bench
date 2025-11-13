#!/usr/bin/env python3
"""
共享工具函数
"""

import csv
from pathlib import Path
from typing import List


def load_device_ids(csv_file: str, device_count: int = None) -> List[str]:
    """
    从 CSV 文件加载 deviceId 列表
    
    Args:
        csv_file: CSV 文件路径
        device_count: 限制读取的设备数量，None 表示读取所有
        
    Returns:
        deviceId 列表
        
    Raises:
        FileNotFoundError: CSV 文件不存在
        ValueError: CSV 文件中没有找到 deviceId
    """
    csv_path = Path(csv_file)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 文件不存在: {csv_file}")

    device_ids: List[str] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # 假设 CSV 文件有 'deviceId' 列，如果没有则尝试第一列
        if "deviceId" in reader.fieldnames:
            device_ids = [row["deviceId"] for row in reader]
        else:
            # 如果没有 deviceId 列，使用第一列
            device_ids = [list(row.values())[0] for row in reader]

    if not device_ids:
        raise ValueError("CSV 文件中没有找到 deviceId")

    # 检查 deviceId 数量是否足够
    if device_count is not None:
        if len(device_ids) < device_count:
            raise ValueError(
                f"CSV 文件中只有 {len(device_ids)} 个 deviceId，"
                f"但需要 {device_count} 个"
            )
        # 只使用前 device_count 个
        device_ids = device_ids[:device_count]

    return device_ids

