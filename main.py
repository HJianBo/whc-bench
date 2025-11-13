#!/usr/bin/env python3
"""
HTTP 压力测试工具
支持从 CSV 文件读取 deviceId，并发发送 HTTP POST 请求
"""

import asyncio
import csv
import json
import time
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path
from typing import List

import aiohttp


class StressTester:
    """压力测试器"""

    def __init__(
        self,
        url: str,
        csv_file: str,
        concurrent: int = 10,
        device_count: int = None,
        loop_count: int = 10,
        interval: int = 0,
        timeout: int = 30,
    ):
        self.url = url
        self.csv_file = csv_file
        self.concurrent = concurrent
        self.device_count = device_count
        self.loop_count = loop_count
        self.interval = interval / 1000.0  # 转换为秒
        self.timeout = timeout
        self.device_ids: List[str] = []
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "errors": [],
        }
        # 用于跟踪每个 deviceId 的上次请求时间（用于间隔控制）
        self.last_request_time = defaultdict(float)
        self.interval_lock = asyncio.Lock()

    def load_device_ids(self) -> None:
        """从 CSV 文件加载 deviceId 列表"""
        csv_path = Path(self.csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV 文件不存在: {self.csv_file}")

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # 假设 CSV 文件有 'deviceId' 列，如果没有则尝试第一列
            if "deviceId" in reader.fieldnames:
                self.device_ids = [row["deviceId"] for row in reader]
            else:
                # 如果没有 deviceId 列，使用第一列
                self.device_ids = [list(row.values())[0] for row in reader]

        if not self.device_ids:
            raise ValueError("CSV 文件中没有找到 deviceId")

        # 检查 deviceId 数量是否足够
        if self.device_count is not None:
            if len(self.device_ids) < self.device_count:
                raise ValueError(
                    f"CSV 文件中只有 {len(self.device_ids)} 个 deviceId，"
                    f"但需要 {self.device_count} 个"
                )
            # 只使用前 device_count 个
            self.device_ids = self.device_ids[: self.device_count]

        print(f"已加载 {len(self.device_ids)} 个 deviceId")

    async def send_request(
        self, session: aiohttp.ClientSession, device_id: str
    ) -> dict:
        """发送单个 HTTP POST 请求"""
        payload = {
            "command": json.dumps(
                {
                    "cmd": "bench",
                    "paras": {"timestamp": int(time.time() * 1000)},
                    "serviceId": "bench",
                }
            ),
            "commandType": 1,
            "deviceId": device_id,
            "expire": 5,
            "qos": 1,
        }

        start_time = time.time()
        try:
            async with session.post(
                self.url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as response:
                elapsed = time.time() - start_time
                response_text = await response.text()
                return {
                    "device_id": device_id,
                    "status": response.status,
                    "success": 200 <= response.status < 300,
                    "elapsed": elapsed,
                    "response": response_text[:200],  # 限制响应长度
                }
        except Exception as e:
            elapsed = time.time() - start_time
            return {
                "device_id": device_id,
                "status": 0,
                "success": False,
                "elapsed": elapsed,
                "error": str(e),
            }

    async def worker(
        self, session: aiohttp.ClientSession, queue: asyncio.Queue
    ) -> None:
        """工作协程：从队列获取 deviceId 并发送请求"""
        while True:
            item = await queue.get()
            if item is None:  # 结束信号
                break

            device_id, loop_index = item

            # 如果不是第一次循环，需要等待间隔
            if self.interval > 0 and loop_index > 0:
                async with self.interval_lock:
                    # 计算距离上次请求的时间
                    last_time = self.last_request_time[device_id]
                    current_time = time.time()
                    elapsed = current_time - last_time
                    
                    # 如果距离上次请求的时间小于间隔，需要等待
                    if elapsed < self.interval:
                        sleep_time = self.interval - elapsed
                    else:
                        sleep_time = 0
                
                # 在锁外等待，避免阻塞其他 deviceId 的请求
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            result = await self.send_request(session, device_id)
            
            # 更新上次请求时间
            async with self.interval_lock:
                self.last_request_time[device_id] = time.time()
            self.stats["total"] += 1

            if result["success"]:
                self.stats["success"] += 1
            else:
                self.stats["failed"] += 1
                self.stats["errors"].append(result)

            queue.task_done()

    async def run(self) -> None:
        """运行压力测试"""
        # 加载 deviceId
        self.load_device_ids()

        # 计算总请求数：每个 deviceId 发送 loop_count 次
        total_requests = len(self.device_ids) * self.loop_count

        print(f"\n开始压力测试:")
        print(f"  URL: {self.url}")
        print(f"  并发数: {self.concurrent}")
        print(f"  DeviceId 数量: {len(self.device_ids)}")
        print(f"  每个 DeviceId 循环次数: {self.loop_count}")
        print(f"  总请求数: {total_requests}")
        if self.interval > 0:
            print(f"  请求间隔: {self.interval * 1000:.0f}ms")
        print(f"  超时时间: {self.timeout}秒\n")

        # 创建请求队列
        queue = asyncio.Queue(maxsize=self.concurrent * 2)

        # 创建 HTTP 会话
        connector = aiohttp.TCPConnector(limit=self.concurrent)
        async with aiohttp.ClientSession(connector=connector) as session:
            # 启动工作协程
            workers = [
                asyncio.create_task(self.worker(session, queue))
                for _ in range(self.concurrent)
            ]

            # 发送请求：每个 deviceId 发送 loop_count 次
            start_time = time.time()
            for device_id in self.device_ids:
                for loop_index in range(self.loop_count):
                    await queue.put((device_id, loop_index))

            # 等待所有任务完成
            await queue.join()

            # 停止工作协程
            for _ in range(self.concurrent):
                await queue.put(None)
            await asyncio.gather(*workers)

            elapsed_time = time.time() - start_time

        # 打印统计信息
        self.print_stats(elapsed_time)

    def print_stats(self, elapsed_time: float) -> None:
        """打印统计信息"""
        print("\n" + "=" * 60)
        print("压力测试结果")
        print("=" * 60)
        print(f"总请求数: {self.stats['total']}")
        print(f"成功: {self.stats['success']}")
        print(f"失败: {self.stats['failed']}")
        print(f"总耗时: {elapsed_time:.2f} 秒")
        if self.stats["total"] > 0:
            print(f"QPS: {self.stats['total'] / elapsed_time:.2f}")
            print(f"成功率: {self.stats['success'] / self.stats['total'] * 100:.2f}%")

        if self.stats["errors"]:
            print(f"\n前 10 个错误:")
            for error in self.stats["errors"][:10]:
                print(f"  DeviceId: {error['device_id']}")
                if "error" in error:
                    print(f"    错误: {error['error']}")
                else:
                    print(f"    状态码: {error['status']}")
        print("=" * 60)


def main():
    """主函数"""
    parser = ArgumentParser(description="HTTP 压力测试工具")
    parser.add_argument(
        "--url",
        required=True,
        help="目标 URL",
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="CSV 文件路径",
    )
    parser.add_argument(
        "-C",
        "--concurrent",
        type=int,
        default=10,
        help="并发数 (默认: 10)",
    )
    parser.add_argument(
        "-c",
        "--device-count",
        type=int,
        default=None,
        dest="device_count",
        help="DeviceId 数量，读取 CSV 文件的前 N 个 ID (默认: 使用所有)",
    )
    parser.add_argument(
        "-n",
        "--loop-count",
        type=int,
        default=10,
        dest="loop_count",
        help="每个 DeviceId 的循环次数 (默认: 10)",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=0,
        help="每个循环发送的间隔（毫秒）(默认: 0)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=30,
        help="请求超时时间（秒）(默认: 30)",
    )

    args = parser.parse_args()

    tester = StressTester(
        url=args.url,
        csv_file=args.csv,
        concurrent=args.concurrent,
        device_count=args.device_count,
        loop_count=args.loop_count,
        interval=args.interval,
        timeout=args.timeout,
    )

    try:
        asyncio.run(tester.run())
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
    except Exception as e:
        print(f"\n错误: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
