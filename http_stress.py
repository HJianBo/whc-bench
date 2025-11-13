#!/usr/bin/env python3
"""
HTTP 压力测试工具
支持从 CSV 文件读取 deviceId，并发发送 HTTP POST 请求
"""

import asyncio
import json
import sys
import time
from argparse import ArgumentParser, SUPPRESS
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import List

import aiohttp

from utils import load_device_ids


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
        emqx: bool = False,
        edgenodeid: str = None,
    ):
        self.url = url
        self.csv_file = csv_file
        self.concurrent = concurrent
        self.device_count = device_count
        self.loop_count = loop_count
        self.interval = interval / 1000.0  # 转换为秒
        self.timeout = timeout
        self.emqx = emqx
        self.edgenodeid = edgenodeid
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
        self.device_ids = load_device_ids(self.csv_file, self.device_count)
        print(f"已加载 {len(self.device_ids)} 个 deviceId")

    async def send_request(
        self, session: aiohttp.ClientSession, device_id: str
    ) -> dict:
        """发送单个 HTTP POST 请求"""
        start_time = time.time()
        try:
            if self.emqx:
                # EMQX 格式的请求
                # 生成 eventTime (UTC+8)
                tz_beijing = timezone(timedelta(hours=8))
                event_time = datetime.now(tz_beijing).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+08:00"
                
                # 在实际发送请求前生成 timestamp，确保时间戳更准确
                timestamp_ns = time.time_ns()
                
                # 构建 payload 字符串（JSON 字符串）
                payload_data = {
                    "msgId": f"dn_{device_id}",
                    "mid": int(time.time() * 1000) % 100000,
                    "serviceId": "bench",
                    "deviceId": device_id,
                    "businessID": f"{device_id}_1001",
                    "cmd": "bench",
                    "expire": -1,
                    "paras": {"timestamp": timestamp_ns},
                    "eventTime": event_time,
                }
                
                payload = {
                    "payload": json.dumps(payload_data),
                    "properties": {
                        "message_expiry_interval": 5
                    },
                    "qos": 1,
                    "topic": f"/v1/devices/{device_id}/command"
                }
                
                headers = {
                    "Authorization": "Basic YXBpOmtleQ==",
                    "Content-Type": "application/json",
                    "Traceparent": "00-419c5f5655c530b810eb3ad38121196b-24c1e1b6639667af-01",
                }
                
                async with session.post(
                    self.url,
                    json=payload,
                    headers=headers,
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
            else:
                # 原始格式的请求
                # 在实际发送请求前生成 timestamp，确保时间戳更准确
                timestamp_ns = time.time_ns()
                
                payload = {
                    "command": json.dumps(
                        {
                            "cmd": "bench",
                            "paras": {"timestamp": timestamp_ns},
                            "serviceId": "bench",
                        }
                    ),
                    "commandType": 1,
                    "deviceId": device_id,
                    "expire": 5,
                    "qos": 1,
                }
                
                headers = {}
                if self.edgenodeid:
                    headers["edgeNodeId"] = self.edgenodeid
                
                async with session.post(
                    self.url,
                    json=payload,
                    headers=headers if headers else None,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    elapsed = time.time() - start_time
                    response_text = await response.text()
                    
                    # 检查 HTTP 状态码
                    http_success = 200 <= response.status < 300
                    
                    # 非 emqx 模式下，需要检查业务 code
                    business_success = False
                    if http_success:
                        try:
                            response_json = json.loads(response_text)
                            # 检查业务 code 是否为 0
                            if isinstance(response_json, dict) and response_json.get("code") == 0:
                                business_success = True
                            else:
                                # HTTP 成功但业务 code 不为 0，打印响应内容
                                print(f"[ERROR] Device {device_id}: HTTP {response.status} but business code is not 0. Response: {response_text[:500]}")
                        except (json.JSONDecodeError, AttributeError, TypeError) as e:
                            # JSON 解析失败或格式不正确，认为业务失败
                            print(f"[ERROR] Device {device_id}: Failed to parse JSON response. Error: {e}. Response: {response_text[:500]}")
                    else:
                        # HTTP 状态码不成功，打印响应内容
                        print(f"[ERROR] Device {device_id}: HTTP {response.status}. Response: {response_text[:500]}")
                    
                    return {
                        "device_id": device_id,
                        "status": response.status,
                        "success": http_success and business_success,
                        "elapsed": elapsed,
                        "response": response_text[:500],  # 增加响应长度以便调试
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
        print(f"  模式: {'EMQX' if self.emqx else '标准'}")
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

            # 发送请求：先发送所有设备的第一条消息，然后是第二条，以此类推
            # 这样可以确保所有设备几乎同时收到相同轮次的消息
            start_time = time.time()
            for loop_index in range(self.loop_count):
                for device_id in self.device_ids:
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
                    if "response" in error:
                        print(f"    响应内容: {error['response']}")
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
    parser.add_argument(
        "--emqx",
        action="store_true",
        help=SUPPRESS,
    )
    parser.add_argument(
        "--edgenodeid",
        type=str,
        default=None,
        help="Edge Node ID，用于非 EMQX 模式的 HTTP Header",
    )

    args = parser.parse_args()
    
    # 非 emqx 模式下，edgenodeid 是必需的
    if not args.emqx and not args.edgenodeid:
        parser.error("--edgenodeid 参数在非 EMQX 模式下是必需的")

    tester = StressTester(
        url=args.url,
        csv_file=args.csv,
        concurrent=args.concurrent,
        device_count=args.device_count,
        loop_count=args.loop_count,
        interval=args.interval,
        timeout=args.timeout,
        emqx=args.emqx,
        edgenodeid=args.edgenodeid,
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
    sys.exit(main())

