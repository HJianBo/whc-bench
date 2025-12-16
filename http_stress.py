#!/usr/bin/env python3
"""
HTTP 压力测试工具
支持从 CSV 文件读取 deviceId，并发发送 HTTP POST 请求
"""

import asyncio
import json
import sys
import threading
import time
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from queue import Queue
from typing import List, Dict, Any
from uuid import uuid4

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
        edgenodeid: str = None,
        productid: str = None,
        emqx: bool = False,
        sk: str = None,
        threads: int = 1,  # 新增：线程数，默认为 1（单线程模式）
    ):
        self.url = url
        self.csv_file = csv_file
        self.concurrent = concurrent
        self.device_count = device_count
        self.loop_count = loop_count
        self.interval = interval / 1000.0  # 转换为秒
        self.timeout = timeout
        self.edgenodeid = edgenodeid
        self.productid = productid
        self.emqx = emqx
        self.sk = sk
        self.threads = threads  # 线程数
        self.device_ids: List[str] = []
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "errors": [],
            "latencies": [],  # 存储所有请求的延迟（秒）
        }
        # 用于跟踪每个 deviceId 的上次请求时间（用于间隔控制）
        self.last_request_time = defaultdict(float)
        self.interval_lock = asyncio.Lock()  # 单线程模式使用
        self.stats_lock = asyncio.Lock()  # 用于保护统计数据的并发访问（单线程模式）
        # 多线程模式下的线程安全锁
        self.thread_stats_lock = threading.Lock()  # 用于多线程模式下的统计保护
        self.thread_interval_lock = threading.Lock()  # 用于多线程模式下的间隔控制

    def load_device_ids(self) -> None:
        """从 CSV 文件加载 deviceId 列表"""
        self.device_ids = load_device_ids(self.csv_file, self.device_count)
        print(f"已加载 {len(self.device_ids)} 个 deviceId")

    def _build_emqx_payload(self, device_id: str, loop_index: int) -> dict:
        """构建 EMQX 格式的 payload"""
        # 在实际发送请求前生成 timestamp，确保时间戳更准确
        timestamp_ns = time.time_ns()
        
        # 生成 mid (使用 loop_index + 1，因为通常从 1 开始)
        mid = loop_index + 1
        
        # 生成 msgId (使用 UUID)
        msg_id = str(uuid4()).replace("-", "")
        
        # 生成 eventTime (ISO 8601 格式，+08:00 时区)
        tz = timezone(timedelta(hours=8))
        event_time = datetime.now(tz).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+08:00"
        
        # 构建 payload 字符串
        payload_data = {
            "cmd": "bench",
            "deviceId": device_id,
            "eventTime": event_time,
            "expire": 5,
            "mid": mid,
            "msgId": msg_id,
            "paras": {"timestamp": timestamp_ns},
            "serviceId": "bench",
        }
        
        # 构建 EMQX 消息体
        emqx_payload = {
            "topic": f"/v1/devices/{device_id}/command",
            "retain": False,
            "qos": 1,
            "properties": {
                "user_properties": {
                    "businessID": f"{device_id}_{mid}"
                },
                "message_expiry_interval": 5
            },
            "payload_encoding": "plain",
            "payload": json.dumps(payload_data)
        }
        
        return emqx_payload

    def _build_standard_payload(self, device_id: str) -> dict:
        """构建标准格式的 payload"""
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
            "gatewayId": device_id,
            "expire": 5,
            "qos": 1,
        }

        # 如果提供了 productId，添加到 payload 中
        if self.productid:
            payload["deviceProductId"] = self.productid
            payload["gatewayProductId"] = self.productid
        
        return payload

    async def send_request(
        self, session: aiohttp.ClientSession, device_id: str, loop_index: int = 0
    ) -> dict:
        """发送单个 HTTP POST 请求"""
        start_time = time.time()
        try:
            # 根据模式构建不同的 payload
            if self.emqx:
                payload = self._build_emqx_payload(device_id, loop_index)
            else:
                payload = self._build_standard_payload(device_id)

            # 构建 headers
            headers = {}
            if self.emqx:
                # EMQX 模式：需要 Authorization Basic <sk>
                if self.sk:
                    # sk 可能已经是 base64 编码的认证字符串，直接使用
                    headers["Authorization"] = f"Basic {self.sk}"
            else:
                # 标准模式：需要 edgeNodeId
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
                
                # 根据模式判断成功条件
                if self.emqx:
                    # EMQX 模式：只检查 HTTP 状态码
                    success = http_success
                    if not http_success:
                        print(f"[ERROR] Device {device_id}: HTTP {response.status}. Response: {response_text[:500]}")
                else:
                    # 标准模式：需要检查业务 code
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
                    success = http_success and business_success
                
                return {
                    "device_id": device_id,
                    "status": response.status,
                    "success": success,
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

            result = await self.send_request(session, device_id, loop_index)
            
            # 更新上次请求时间
            async with self.interval_lock:
                self.last_request_time[device_id] = time.time()
            
            # 更新统计数据（需要线程安全）
            async with self.stats_lock:
                self.stats["total"] += 1
                self.stats["latencies"].append(result["elapsed"])
                if result["success"]:
                    self.stats["success"] += 1
                else:
                    self.stats["failed"] += 1
                    self.stats["errors"].append(result)

            queue.task_done()

    def _update_stats_thread_safe(self, result: Dict[str, Any]) -> None:
        """线程安全的统计更新（用于多线程模式）"""
        with self.thread_stats_lock:
            self.stats["total"] += 1
            self.stats["latencies"].append(result["elapsed"])
            if result["success"]:
                self.stats["success"] += 1
            else:
                self.stats["failed"] += 1
                self.stats["errors"].append(result)

    async def _run_single_threaded(self) -> None:
        """单线程模式运行（原有逻辑）"""
        # 加载 deviceId
        self.load_device_ids()

        # 计算总请求数：每个 deviceId 发送 loop_count 次
        total_requests = len(self.device_ids) * self.loop_count

        print(f"\n开始压力测试 (单线程模式):")
        print(f"  模式: {'EMQX' if self.emqx else '标准'}")
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

        # 创建 HTTP 会话和连接池
        connector = aiohttp.TCPConnector(
            limit=max(100, self.concurrent * 2),
            limit_per_host=self.concurrent,
            ttl_dns_cache=300,
            force_close=False,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(
            total=self.timeout,
            connect=5,
            sock_read=10,
        )
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            read_bufsize=65536,
        ) as session:
            workers = [
                asyncio.create_task(self.worker(session, queue))
                for _ in range(self.concurrent)
            ]

            start_time = time.time()
            for loop_index in range(self.loop_count):
                for device_id in self.device_ids:
                    await queue.put((device_id, loop_index))

            await queue.join()

            for _ in range(self.concurrent):
                await queue.put(None)
            await asyncio.gather(*workers)

            elapsed_time = time.time() - start_time

        self.print_stats(elapsed_time)

    def _thread_worker(self, task_queue: Queue, thread_id: int) -> None:
        """线程工作函数：每个线程运行自己的事件循环"""
        # 为每个线程创建独立的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self._thread_async_worker(task_queue, thread_id))
        finally:
            loop.close()

    async def _thread_async_worker(self, task_queue: Queue, thread_id: int) -> None:
        """线程内的异步工作函数"""
        # 每个线程创建自己的连接池和会话
        connector = aiohttp.TCPConnector(
            limit=max(100, self.concurrent * 2),
            limit_per_host=self.concurrent,
            ttl_dns_cache=300,
            force_close=False,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(
            total=self.timeout,
            connect=5,
            sock_read=10,
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            read_bufsize=65536,
        ) as session:
            # 创建该线程的异步队列
            async_queue = asyncio.Queue(maxsize=self.concurrent * 2)
            
            # 启动工作协程
            workers = [
                asyncio.create_task(self._thread_worker_coroutine(session, async_queue, thread_id))
                for _ in range(self.concurrent)
            ]
            
            # 从线程队列获取任务并放入异步队列
            async def task_loader():
                task_count = 0
                while True:
                    try:
                        # 非阻塞获取任务
                        item = task_queue.get_nowait()
                        if item is None:  # 结束信号
                            break
                        await async_queue.put(item)
                        task_count += 1
                    except Exception:
                        # 队列为空（Empty 异常），等待一小段时间
                        await asyncio.sleep(0.01)
                        # 如果队列为空且异步队列也为空，则退出
                        if task_queue.empty() and async_queue.empty():
                            # 再等待一下，确保没有新任务
                            await asyncio.sleep(0.1)
                            if task_queue.empty():
                                break
                
                # 发送停止信号给工作协程
                for _ in range(self.concurrent):
                    await async_queue.put(None)
            
            loader_task = asyncio.create_task(task_loader())
            
            # 等待所有协程和加载器完成
            await asyncio.gather(*workers, loader_task, return_exceptions=True)

    async def _thread_worker_coroutine(
        self, session: aiohttp.ClientSession, queue: asyncio.Queue, thread_id: int
    ) -> None:
        """线程内的工作协程"""
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            
            if item is None:  # 结束信号
                break

            device_id, loop_index = item

            # 间隔控制（多线程模式使用线程锁）
            if self.interval > 0 and loop_index > 0:
                with self.thread_interval_lock:
                    last_time = self.last_request_time[device_id]
                    current_time = time.time()
                    elapsed = current_time - last_time
                    if elapsed < self.interval:
                        sleep_time = self.interval - elapsed
                    else:
                        sleep_time = 0
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            result = await self.send_request(session, device_id, loop_index)
            
            # 更新上次请求时间（多线程模式使用线程锁）
            with self.thread_interval_lock:
                self.last_request_time[device_id] = time.time()
            
            # 线程安全的统计更新
            self._update_stats_thread_safe(result)

            queue.task_done()

    async def run(self) -> None:
        """运行压力测试"""
        if self.threads == 1:
            # 单线程模式（原有逻辑）
            await self._run_single_threaded()
        else:
            # 多线程模式
            await self._run_multi_threaded()

    async def _run_multi_threaded(self) -> None:
        """多线程模式运行"""
        # 加载 deviceId
        self.load_device_ids()

        # 计算总请求数
        total_requests = len(self.device_ids) * self.loop_count

        print(f"\n开始压力测试 (多线程模式):")
        print(f"  模式: {'EMQX' if self.emqx else '标准'}")
        print(f"  URL: {self.url}")
        print(f"  线程数: {self.threads}")
        print(f"  每线程并发数: {self.concurrent}")
        print(f"  总并发数: {self.threads * self.concurrent}")
        print(f"  DeviceId 数量: {len(self.device_ids)}")
        print(f"  每个 DeviceId 循环次数: {self.loop_count}")
        print(f"  总请求数: {total_requests}")
        print(f"  每个线程的连接池: {self.concurrent} 个连接/主机")
        print(f"  总连接池数: {self.threads} 个（每个线程一个）")
        print(f"  总连接数限制: {self.threads * self.concurrent} 个连接/主机")
        if self.interval > 0:
            print(f"  请求间隔: {self.interval * 1000:.0f}ms")
        print(f"  超时时间: {self.timeout}秒\n")

        # 创建任务队列（线程安全的 Queue）
        task_queue = Queue()
        
        # 准备所有任务
        tasks = []
        for loop_index in range(self.loop_count):
            for device_id in self.device_ids:
                tasks.append((device_id, loop_index))
        
        # 将任务放入队列
        for task in tasks:
            task_queue.put(task)
        
        # 创建并启动线程
        threads = []
        start_time = time.time()
        
        for thread_id in range(self.threads):
            thread = threading.Thread(
                target=self._thread_worker,
                args=(task_queue, thread_id),
                name=f"WorkerThread-{thread_id}"
            )
            thread.start()
            threads.append(thread)
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
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
        
        # 打印延迟统计信息
        if self.stats["latencies"]:
            latencies = sorted(self.stats["latencies"])
            total_latencies = len(latencies)
            avg_latency = sum(latencies) / total_latencies
            min_latency = latencies[0]
            max_latency = latencies[-1]
            median_latency = latencies[total_latencies // 2]
            p95_index = int(total_latencies * 0.95)
            p99_index = int(total_latencies * 0.99)
            p95_latency = latencies[p95_index] if p95_index < total_latencies else latencies[-1]
            p99_latency = latencies[p99_index] if p99_index < total_latencies else latencies[-1]
            
            print(f"\n延迟统计 (请求到响应时间):")
            print(f"  平均延迟: {avg_latency * 1000:.2f} ms")
            print(f"  最小延迟: {min_latency * 1000:.2f} ms")
            print(f"  最大延迟: {max_latency * 1000:.2f} ms")
            print(f"  中位数延迟: {median_latency * 1000:.2f} ms")
            print(f"  P95 延迟: {p95_latency * 1000:.2f} ms")
            print(f"  P99 延迟: {p99_latency * 1000:.2f} ms")

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
        help="每线程并发数（协程数）(默认: 10)",
    )
    parser.add_argument(
        "-T",
        "--threads",
        type=int,
        default=1,
        help="线程数，用于充分利用多核 CPU (默认: 1，单线程模式)",
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
        "--edgenodeid",
        type=str,
        default=None,
        help="Edge Node ID，用于 HTTP Header",
    )
    parser.add_argument(
        "--productid",
        type=str,
        default=None,
        help="Product ID，用于原始格式的请求 payload",
    )
    parser.add_argument(
        "--emqx",
        action="store_true",
        help="启用 EMQX 模式",
    )
    parser.add_argument(
        "--sk",
        type=str,
        default=None,
        help="EMQX Secret Key，用于 Basic 认证（仅在 --emqx 模式下必需）",
    )

    args = parser.parse_args()
    
    # 参数验证
    if args.emqx:
        # EMQX 模式下，sk 是必需的
        if not args.sk:
            parser.error("--sk 参数在 --emqx 模式下是必需的")
    else:
        # 标准模式下，edgenodeid 是必需的
        if not args.edgenodeid:
            parser.error("--edgenodeid 参数在标准模式下是必需的")

    tester = StressTester(
        url=args.url,
        csv_file=args.csv,
        concurrent=args.concurrent,
        device_count=args.device_count,
        loop_count=args.loop_count,
        interval=args.interval,
        timeout=args.timeout,
        edgenodeid=args.edgenodeid,
        productid=args.productid,
        emqx=args.emqx,
        sk=args.sk,
        threads=args.threads,
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

