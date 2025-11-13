#!/usr/bin/env python3
"""
MQTT 客户端程序
支持多个设备并发连接到 MQTT Broker，订阅命令主题并测量消息时延
"""

import asyncio
import json
import time
from argparse import ArgumentParser
from collections import defaultdict
from typing import Dict, List

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
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.device_id = device_id
        self.password = password
        self.keepalive = keepalive
        self.topic = f"/v1/devices/{device_id}/command" if device_id else None
        self.stats = {
            "messages_received": 0,
            "latencies": [],
            "errors": [],
        }
        self.running = True

    async def on_message(self, message) -> None:
        """处理接收到的消息"""
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
                    # 计算时延（毫秒）
                    current_time_ms = int(time.time() * 1000)
                    latency_ms = current_time_ms - timestamp
                    
                    self.stats["messages_received"] += 1
                    self.stats["latencies"].append(latency_ms)
                    
                    print(
                        f"[{self.device_id}] 收到消息，时延: {latency_ms}ms "
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

    def load_device_ids(self) -> None:
        """从 CSV 文件加载 deviceId 列表"""
        self.device_ids = load_device_ids(self.csv_file, self.device_count)
        print(f"已加载 {len(self.device_ids)} 个 deviceId")

    async def start_clients(self) -> None:
        """启动所有客户端"""
        self.load_device_ids()
        
        print(f"\n开始连接 MQTT Broker:")
        print(f"  Broker: {self.broker_host}:{self.broker_port}")
        print(f"  设备数量: {len(self.device_ids)}")
        print(f"  密码: {self.password}")
        print()

        # 创建所有客户端并启动连接任务
        for device_id in self.device_ids:
            client = MQTTClient(
                broker_host=self.broker_host,
                broker_port=self.broker_port,
                device_id=device_id,
                password=self.password,
                keepalive=self.keepalive,
            )
            self.clients.append(client)
            task = asyncio.create_task(client.connect_and_subscribe())
            self.tasks.append(task)

        # 等待所有任务完成（通常不会完成，除非出错）
        try:
            await asyncio.gather(*self.tasks)
        except Exception as e:
            print(f"客户端运行出错: {e}")

    def stop_all(self) -> None:
        """停止所有客户端"""
        print("\n正在停止所有客户端...")
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
                print(f"  平均时延: {stats['latency_avg']:.2f}ms")
                print(f"  最小时延: {stats['latency_min']:.2f}ms")
                print(f"  最大时延: {stats['latency_max']:.2f}ms")
                print(f"  P50 时延: {stats['latency_p50']:.2f}ms")
                print(f"  P95 时延: {stats['latency_p95']:.2f}ms")
                print(f"  P99 时延: {stats['latency_p99']:.2f}ms")
        
        print("\n" + "-" * 80)
        print("总计:")
        print(f"  总消息数: {total_messages}")
        print(f"  总错误数: {total_errors}")
        
        if all_latencies:
            print(f"\n全局统计:")
            print(f"  平均时延: {sum(all_latencies) / len(all_latencies):.2f}ms")
            print(f"  最小时延: {min(all_latencies):.2f}ms")
            print(f"  最大时延: {max(all_latencies):.2f}ms")
            sorted_latencies = sorted(all_latencies)
            print(f"  P50 时延: {sorted_latencies[len(sorted_latencies) // 2]:.2f}ms")
            print(f"  P95 时延: {sorted_latencies[int(len(sorted_latencies) * 0.95)]:.2f}ms")
            print(f"  P99 时延: {sorted_latencies[int(len(sorted_latencies) * 0.99)]:.2f}ms")
        
        print("=" * 80)


async def main_async(args) -> int:
    """异步主函数"""
    manager = MQTTClientManager(
        broker_host=args.broker,
        broker_port=args.port,
        csv_file=args.csv,
        device_count=args.device_count,
        password=args.password,
        keepalive=args.keepalive,
    )

    try:
        # 启动所有客户端
        await manager.start_clients()
    except KeyboardInterrupt:
        print("\n\n收到中断信号，正在停止...")
        manager.stop_all()
        await asyncio.sleep(1)  # 等待任务清理
        manager.print_stats()
        return 0
    except Exception as e:
        print(f"\n错误: {e}")
        manager.stop_all()
        return 1

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

    try:
        exit_code = asyncio.run(main_async(args))
        return exit_code
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
        return 0
    except Exception as e:
        print(f"\n错误: {e}")
        return 1


if __name__ == "__main__":
    exit(main())

