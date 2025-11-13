#!/usr/bin/env python3
"""
MQTT 客户端程序
支持多个设备并发连接到 MQTT Broker，订阅命令主题并测量消息时延
"""

import asyncio
import csv
import json
import os
import signal
import time
from argparse import ArgumentParser
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from aiomqtt import Client
from aiomqtt.exceptions import MqttError

from utils import load_device_ids


class MQTTClient:
    """MQTT 客户端"""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        device_id: str = None,
        password: str = "123456",
        keepalive: int = 60,
        latency_queue: Optional[asyncio.Queue] = None,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.device_id = device_id
        self.password = password
        self.keepalive = keepalive
        self.topic = f"/v1/devices/{device_id}/command" if device_id else None
        self.latency_queue = latency_queue
        self.stats = {
            "messages_received": 0,
            "latencies": [],
            "errors": [],
        }
        self.running = True

    async def on_message(self, message) -> None:
        """处理接收到的消息"""
        # 在函数开始时就记录接收时间，确保时间戳精确反映消息到达时间
        # 这样可以排除后续解析和处理时间对延迟计算的影响
        receive_time_ns = time.time_ns()
        
        try:
            # 解析消息 payload
            payload_str = message.payload.decode("utf-8")
            payload = json.loads(payload_str)
            
            # 获取消息中的 timestamp
            if isinstance(payload, dict):
                # 尝试从不同位置获取 timestamp
                timestamp = None
                if "timestamp" in payload:
                    timestamp = payload["timestamp"]
                elif "paras" in payload and isinstance(payload["paras"], dict):
                    timestamp = payload["paras"].get("timestamp")
                elif "command" in payload:
                    # 如果 command 是字符串，尝试解析
                    try:
                        cmd_data = json.loads(payload["command"])
                        if isinstance(cmd_data, dict):
                            if "paras" in cmd_data and isinstance(cmd_data["paras"], dict):
                                timestamp = cmd_data["paras"].get("timestamp")
                            elif "timestamp" in cmd_data:
                                timestamp = cmd_data["timestamp"]
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                if timestamp:
                    # 计算时延（纳秒）
                    # 使用函数开始时就记录的时间戳，排除解析时间的影响
                    latency_ns = receive_time_ns - timestamp
                    
                    self.stats["messages_received"] += 1
                    self.stats["latencies"].append(latency_ns)
                    
                    # 转换为带小数的毫秒显示
                    latency_ms = latency_ns / 1_000_000.0
                    
                    # 异步将延迟数据放入队列（不阻塞消息循环）
                    if self.latency_queue:
                        asyncio.create_task(
                            self.latency_queue.put(
                                (self.device_id, self.stats["messages_received"], latency_ms)
                            )
                        )
                    
                    print(
                        f"[{self.device_id}] 收到消息，时延: {latency_ms:.3f}ms "
                        f"(消息数: {self.stats['messages_received']})"
                    )
                else:
                    print(f"[{self.device_id}] 收到消息，但未找到 timestamp")
                    self.stats["errors"].append(
                        {"error": "timestamp not found", "payload": payload_str[:200]}
                    )
            else:
                print(f"[{self.device_id}] 收到消息，但 payload 格式不正确")
                self.stats["errors"].append(
                    {"error": "invalid payload format", "payload": payload_str[:200]}
                )
        except Exception as e:
            print(f"[{self.device_id}] 处理消息时出错: {e}")
            self.stats["errors"].append({"error": str(e)})

    async def connect_and_subscribe(self) -> None:
        """连接 MQTT Broker 并订阅主题"""
        try:
            async with Client(
                hostname=self.broker_host,
                port=self.broker_port,
                identifier=self.device_id,
                username=self.device_id,
                password=self.password,
                keepalive=self.keepalive,
            ) as client:
                print(f"[{self.device_id}] 已连接到 MQTT Broker {self.broker_host}:{self.broker_port}")
                
                # 订阅主题
                await client.subscribe(self.topic, qos=1)
                print(f"[{self.device_id}] 已订阅主题: {self.topic}")
                
                # 接收消息
                async for message in client.messages:
                    if not self.running:
                        break
                    await self.on_message(message)
        except MqttError as e:
            print(f"[{self.device_id}] MQTT 错误: {e}")
            self.stats["errors"].append({"error": f"MQTT error: {e}"})
        except Exception as e:
            print(f"[{self.device_id}] 连接错误: {e}")
            self.stats["errors"].append({"error": str(e)})

    def stop(self) -> None:
        """停止客户端"""
        self.running = False

    def get_stats(self) -> Dict:
        """获取统计信息"""
        latencies = self.stats["latencies"]
        stats = {
            "device_id": self.device_id,
            "messages_received": self.stats["messages_received"],
            "errors_count": len(self.stats["errors"]),
        }
        
        if latencies:
            stats["latency_avg"] = sum(latencies) / len(latencies)
            stats["latency_min"] = min(latencies)
            stats["latency_max"] = max(latencies)
            stats["latency_p50"] = sorted(latencies)[len(latencies) // 2]
            stats["latency_p95"] = sorted(latencies)[int(len(latencies) * 0.95)]
            stats["latency_p99"] = sorted(latencies)[int(len(latencies) * 0.99)]
        
        return stats


class MQTTClientManager:
    """MQTT 客户端管理器，管理多个客户端"""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        csv_file: str = None,
        device_count: int = None,
        password: str = "123456",
        keepalive: int = 60,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.csv_file = csv_file
        self.device_count = device_count
        self.password = password
        self.keepalive = keepalive
        self.device_ids: List[str] = []
        self.clients: List[MQTTClient] = []
        self.tasks: List[asyncio.Task] = []
        
        # CSV 输出相关
        current_timestamp = int(time.time())
        self.output_csv_file = f"output_{current_timestamp}.csv"
        self.latency_queue: asyncio.Queue = asyncio.Queue()
        self.csv_writer_task: Optional[asyncio.Task] = None
        self.csv_writer_running = True

    def load_device_ids(self) -> None:
        """从 CSV 文件加载 deviceId 列表"""
        self.device_ids = load_device_ids(self.csv_file, self.device_count)
        print(f"已加载 {len(self.device_ids)} 个 deviceId")

    async def _csv_writer_worker(self) -> None:
        """CSV 写入后台任务，从队列中获取延迟数据并写入文件"""
        # 初始化 CSV 文件，写入表头
        def write_header():
            with open(self.output_csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["deviceId", "no", "latency"])

        await asyncio.to_thread(write_header)
        print(f"延迟统计将输出到: {self.output_csv_file}")

        # 批量写入缓冲区
        buffer: List[Tuple[str, int, float]] = []
        buffer_size = 100  # 每 100 条记录写入一次

        def write_batch():
            """写入缓冲区数据到文件"""
            if buffer:
                with open(
                    self.output_csv_file, "a", newline="", encoding="utf-8"
                ) as f:
                    writer = csv.writer(f)
                    writer.writerows(buffer)
                buffer.clear()

        while self.csv_writer_running:
            try:
                # 等待队列中的数据，设置超时以便定期刷新缓冲区
                try:
                    data = await asyncio.wait_for(
                        self.latency_queue.get(), timeout=1.0
                    )
                    buffer.append(data)
                except asyncio.TimeoutError:
                    # 超时后检查是否有缓冲数据需要写入
                    if buffer:
                        await asyncio.to_thread(write_batch)
                    continue

                # 当缓冲区达到指定大小时，批量写入
                if len(buffer) >= buffer_size:
                    await asyncio.to_thread(write_batch)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"CSV 写入错误: {e}")

        # 处理完队列中剩余的所有数据
        while not self.latency_queue.empty():
            try:
                data = self.latency_queue.get_nowait()
                buffer.append(data)
            except asyncio.QueueEmpty:
                break

        # 写入剩余的缓冲数据
        if buffer:
            await asyncio.to_thread(write_batch)

    async def start_clients(self) -> None:
        """启动所有客户端"""
        self.load_device_ids()
        
        print(f"\n开始连接 MQTT Broker:")
        print(f"  Broker: {self.broker_host}:{self.broker_port}")
        print(f"  设备数量: {len(self.device_ids)}")
        print(f"  密码: {self.password}")
        print()

        # 启动 CSV 写入后台任务
        self.csv_writer_task = asyncio.create_task(self._csv_writer_worker())

        # 创建所有客户端并启动连接任务
        for device_id in self.device_ids:
            client = MQTTClient(
                broker_host=self.broker_host,
                broker_port=self.broker_port,
                device_id=device_id,
                password=self.password,
                keepalive=self.keepalive,
                latency_queue=self.latency_queue,
            )
            self.clients.append(client)
            task = asyncio.create_task(client.connect_and_subscribe())
            self.tasks.append(task)

        # 等待所有任务完成（通常不会完成，除非出错）
        # KeyboardInterrupt 和 CancelledError 需要向上传播，所以不在这里捕获
        try:
            await asyncio.gather(*self.tasks, self.csv_writer_task)
        except asyncio.CancelledError:
            # CancelledError 需要向上传播，让 main_async 处理
            raise
        except Exception as e:
            # 只捕获普通异常
            print(f"客户端运行出错: {e}")
            raise

    def stop_all(self) -> None:
        """停止所有客户端"""
        print("\n正在停止所有客户端...")
        # 停止 CSV 写入任务
        self.csv_writer_running = False
        if self.csv_writer_task and not self.csv_writer_task.done():
            self.csv_writer_task.cancel()
        
        # 停止所有客户端
        for client in self.clients:
            client.stop()
        for task in self.tasks:
            if not task.done():
                task.cancel()

    def print_stats(self) -> None:
        """打印统计信息"""
        print("\n" + "=" * 80)
        print("MQTT 客户端统计信息")
        print("=" * 80)
        
        total_messages = 0
        total_errors = 0
        all_latencies = []
        
        for client in self.clients:
            stats = client.get_stats()
            total_messages += stats["messages_received"]
            total_errors += stats["errors_count"]
            if "latency_avg" in stats:
                all_latencies.extend(client.stats["latencies"])
            
            print(f"\n设备: {stats['device_id']}")
            print(f"  收到消息数: {stats['messages_received']}")
            print(f"  错误数: {stats['errors_count']}")
            if "latency_avg" in stats:
                # 将纳秒转换为毫秒显示
                print(f"  平均时延: {stats['latency_avg'] / 1_000_000.0:.3f}ms")
                print(f"  最小时延: {stats['latency_min'] / 1_000_000.0:.3f}ms")
                print(f"  最大时延: {stats['latency_max'] / 1_000_000.0:.3f}ms")
                print(f"  P50 时延: {stats['latency_p50'] / 1_000_000.0:.3f}ms")
                print(f"  P95 时延: {stats['latency_p95'] / 1_000_000.0:.3f}ms")
                print(f"  P99 时延: {stats['latency_p99'] / 1_000_000.0:.3f}ms")
        
        print("\n" + "-" * 80)
        print("总计:")
        print(f"  总消息数: {total_messages}")
        print(f"  总错误数: {total_errors}")
        
        if all_latencies:
            print(f"\n全局统计:")
            # 将纳秒转换为毫秒显示
            avg_ns = sum(all_latencies) / len(all_latencies)
            min_ns = min(all_latencies)
            max_ns = max(all_latencies)
            sorted_latencies = sorted(all_latencies)
            p50_ns = sorted_latencies[len(sorted_latencies) // 2]
            p95_ns = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            p99_ns = sorted_latencies[int(len(sorted_latencies) * 0.99)]
            print(f"  平均时延: {avg_ns / 1_000_000.0:.3f}ms")
            print(f"  最小时延: {min_ns / 1_000_000.0:.3f}ms")
            print(f"  最大时延: {max_ns / 1_000_000.0:.3f}ms")
            print(f"  P50 时延: {p50_ns / 1_000_000.0:.3f}ms")
            print(f"  P95 时延: {p95_ns / 1_000_000.0:.3f}ms")
            print(f"  P99 时延: {p99_ns / 1_000_000.0:.3f}ms")
        
        print("=" * 80)


# 全局变量，用于在信号处理中访问 manager
_manager_instance = None


async def main_async(args) -> int:
    """异步主函数"""
    global _manager_instance
    manager = MQTTClientManager(
        broker_host=args.broker,
        broker_port=args.port,
        csv_file=args.csv,
        device_count=args.device_count,
        password=args.password,
        keepalive=args.keepalive,
    )
    _manager_instance = manager

    try:
        # 启动所有客户端
        await manager.start_clients()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # 用户中断或任务被取消时，执行清理
        print("\n\n收到中断信号，正在停止...")
        manager.stop_all()
        await asyncio.sleep(1)  # 等待任务清理
        manager.print_stats()
        return 0
    except Exception as e:
        print(f"\n错误: {e}")
        manager.stop_all()
        return 1
    finally:
        _manager_instance = None

    return 0


def main():
    """主函数"""
    parser = ArgumentParser(description="MQTT 客户端程序")
    parser.add_argument(
        "--broker",
        required=True,
        help="MQTT Broker 地址",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=1883,
        help="MQTT Broker 端口 (默认: 1883)",
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="CSV 文件路径",
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
        "-p",
        "--password",
        type=str,
        default="123456",
        help="MQTT 密码 (默认: 123456)",
    )
    parser.add_argument(
        "-k",
        "--keepalive",
        type=int,
        default=60,
        help="Keepalive 时间（秒）(默认: 60)",
    )

    args = parser.parse_args()

    def signal_handler(signum, frame):
        """处理 SIGINT 信号（Ctrl+C）"""
        global _manager_instance
        if _manager_instance:
            _manager_instance.stop_all()
        # 设置信号处理为默认行为，让 asyncio.run() 正常处理
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)

    try:
        exit_code = asyncio.run(main_async(args))
        return exit_code
    except KeyboardInterrupt:
        # 如果还有 manager 实例，打印统计信息
        global _manager_instance
        if _manager_instance:
            # 创建一个新的事件循环来运行清理代码
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(asyncio.sleep(1))
                _manager_instance.print_stats()
            finally:
                loop.close()
        return 0
    except Exception as e:
        print(f"\n错误: {e}")
        return 1


if __name__ == "__main__":
    exit(main())

