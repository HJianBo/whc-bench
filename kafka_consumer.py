#!/usr/bin/env python3
"""
Kafka 消费者程序
用于消费 Kafka 消息并测量 Producer 到 Consumer 的时延
"""

import asyncio
import csv
import json
import signal
import sys
import time
from argparse import ArgumentParser
from typing import Dict, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

# 全局变量，用于在信号处理中访问 consumer_manager
_consumer_manager_instance = None


class KafkaConsumerManager:
    """Kafka 消费者管理器"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        csv_file: str = None,
        request_timeout_ms: int = 30000,
        sasl_mechanism: str = None,
        sasl_username: str = None,
        sasl_password: str = None,
        security_protocol: str = None,
        idle_timeout_seconds: float = 10.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.csv_file = csv_file
        self.request_timeout_ms = request_timeout_ms
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.security_protocol = security_protocol
        self.idle_timeout_seconds = idle_timeout_seconds
        
        # 统计数据
        self.stats = {
            "messages_received": 0,
            "message_timestamp_latency": [],  # 消息时间戳到当前时间的延迟
            "tx_start_latency": [],  # tx_start_ts 到当前时间的延迟
            "errors": [],
        }
        
        # CSV 输出相关
        current_timestamp = int(time.time())
        self.output_csv_file = csv_file or f"kafka_latency_{current_timestamp}.csv"
        self.data_queue: asyncio.Queue = asyncio.Queue()
        self.csv_writer_task: Optional[asyncio.Task] = None
        self.csv_writer_running = True
        
        # 消费者实例
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = True
        
        # 超时检测相关
        self.last_message_time: Optional[float] = None
        self.start_consuming_time: Optional[float] = None
        self.timeout_check_task: Optional[asyncio.Task] = None
        self.last_warning_time: Optional[float] = None  # 用于避免重复打印警告

    async def _timeout_checker(self) -> None:
        """超时检测任务，如果指定时间内没有收到新消息，自动停止消费"""
        # 等待开始消费时间被设置
        while self.start_consuming_time is None and self.running:
            await asyncio.sleep(0.1)
        
        if not self.running:
            return
        
        # 定期检查是否超时
        while self.running:
            await asyncio.sleep(1.0)  # 每秒检查一次
            
            current_time = time.time()
            
            # 如果从未收到消息，从开始消费时计时
            if self.last_message_time is None:
                idle_time = current_time - self.start_consuming_time
                # 每5秒打印一次等待状态（避免重复打印）
                if (self.last_warning_time is None or 
                    current_time - self.last_warning_time >= 5.0):
                    remaining = self.idle_timeout_seconds - idle_time
                    if remaining > 0:
                        print(f"等待消息中... (已等待 {idle_time:.1f}秒，剩余 {remaining:.1f}秒)")
                        self.last_warning_time = current_time
                if idle_time >= self.idle_timeout_seconds:
                    print(f"\n{self.idle_timeout_seconds}秒内未收到任何消息，停止消费...")
                    self.stop()
                    # 停止消费者以中断消费循环
                    if self.consumer:
                        try:
                            await self.consumer.stop()
                        except Exception:
                            pass
                    break
            else:
                # 如果收到过消息，从最后一条消息的时间计时
                idle_time = current_time - self.last_message_time
                if idle_time >= self.idle_timeout_seconds:
                    print(f"\n{self.idle_timeout_seconds}秒内未收到新消息，停止消费...")
                    self.stop()
                    # 停止消费者以中断消费循环
                    if self.consumer:
                        try:
                            await self.consumer.stop()
                        except Exception:
                            pass
                    break

    async def _csv_writer_worker(self) -> None:
        """CSV 写入后台任务，从队列中获取延迟数据并写入文件"""
        # 初始化 CSV 文件，写入表头
        def write_header():
            with open(self.output_csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["message_timestamp", "message_timestamp_latency_ms", "tx_start_latency_ms"])

        await asyncio.to_thread(write_header)
        print(f"延迟数据将输出到: {self.output_csv_file}")

        # 批量写入缓冲区
        # 数据类型: (message_timestamp, message_timestamp_latency, tx_start_latency)
        # 如果没有 tx_start_ts Header，tx_start_latency 为 0.0
        buffer: List[Tuple[float, float, float]] = []
        buffer_size = 1000  # 每 1000 条记录写入一次，提高性能

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
                        self.data_queue.get(), timeout=1.0
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
        while not self.data_queue.empty():
            try:
                data = self.data_queue.get_nowait()
                buffer.append(data)
            except asyncio.QueueEmpty:
                break

        # 写入剩余的缓冲数据
        if buffer:
            await asyncio.to_thread(write_batch)

    async def process_message(self, message) -> None:
        """处理接收到的消息"""
        # 更新最后消息时间
        self.last_message_time = time.time()
        
        # 在函数开始时就记录接收时间，确保时间戳精确反映消息到达时间
        receive_time_ns = time.time_ns()
        receive_time_ms = receive_time_ns / 1_000_000.0  # 转换为毫秒

        try:
            # 获取消息时间戳（Kafka 消息的 timestamp）
            # Kafka 消息的 timestamp 单位是毫秒（Unix 时间戳）
            message_timestamp_ms = None
            if message.timestamp:
                # Kafka timestamp 是毫秒
                message_timestamp_ms = message.timestamp
            
            # 如果消息没有 timestamp，尝试从 value 中解析
            if message_timestamp_ms is None and message.value:
                try:
                    payload = json.loads(message.value.decode("utf-8"))
                    if isinstance(payload, dict) and "timestamp" in payload:
                        ts = payload["timestamp"]
                        if isinstance(ts, (int, float)):
                            # 判断是纳秒还是毫秒（纳秒通常 > 1e12，毫秒通常 < 1e12）
                            if ts > 1e12:
                                message_timestamp_ms = ts / 1_000_000.0  # 纳秒转毫秒
                            else:
                                message_timestamp_ms = ts  # 已经是毫秒
                except (json.JSONDecodeError, UnicodeDecodeError, AttributeError, TypeError):
                    pass

            # 获取 Header 中的 tx_start_ts（统一按毫秒级时间戳处理）
            tx_start_ts_ms = None
            if message.headers:
                for header_key, header_value in message.headers:
                    if header_key == "tx_start_ts":
                        try:
                            # 尝试解析为字符串或字节
                            if isinstance(header_value, bytes):
                                tx_start_ts_str = header_value.decode("utf-8")
                            else:
                                tx_start_ts_str = str(header_value)
                            
                            # 解析为数字，统一按毫秒级时间戳处理
                            tx_start_ts_ms = float(tx_start_ts_str)
                        except (ValueError, AttributeError, TypeError, UnicodeDecodeError) as e:
                            print(f"警告: 解析 tx_start_ts header 时出错: {e}")
                            tx_start_ts_ms = None
                        break

            # 计算延迟
            message_timestamp_latency_ms = None
            tx_start_latency_ms = 0.0  # 默认值为 0，如果没有 tx_start_ts Header
            
            if message_timestamp_ms is not None:
                message_timestamp_latency_ms = receive_time_ms - message_timestamp_ms
                self.stats["message_timestamp_latency"].append(message_timestamp_latency_ms)
            
            if tx_start_ts_ms is not None:
                # 有 tx_start_ts Header，计算延迟
                tx_start_latency_ms = receive_time_ms - tx_start_ts_ms
                self.stats["tx_start_latency"].append(tx_start_latency_ms)
            # 如果没有 tx_start_ts Header，tx_start_latency_ms 保持为 0.0

            # 记录到 CSV（记录消息时间戳、消息时间戳延迟、事务开始延迟）
            # 使用毫秒时间戳（与 Kafka timestamp 格式一致）
            message_timestamp_value = message_timestamp_ms if message_timestamp_ms is not None else 0.0
            message_timestamp_latency_value = message_timestamp_latency_ms if message_timestamp_latency_ms is not None else 0.0
            # 事务开始延迟：如果没有 tx_start_ts Header，则为 0.0
            tx_start_latency_value = tx_start_latency_ms
            
            # 异步将数据放入队列（不阻塞消息处理）
            await self.data_queue.put((message_timestamp_value, message_timestamp_latency_value, tx_start_latency_value))

            self.stats["messages_received"] += 1

            # 每 1000 条消息打印一次进度
            if self.stats["messages_received"] % 1000 == 0:
                message_timestamp_latency_str = f"{message_timestamp_latency_ms:.2f}ms" if message_timestamp_latency_ms is not None else "N/A"
                tx_start_latency_str = f"{tx_start_latency_ms:.2f}ms"
                print(
                    f"已消费 {self.stats['messages_received']} 条消息 | "
                    f"消息时间戳延迟: {message_timestamp_latency_str} | 事务开始延迟: {tx_start_latency_str}"
                )

        except Exception as e:
            print(f"处理消息时出错: {e}")
            self.stats["errors"].append({"error": str(e), "message": str(message)[:200]})

    async def consume_messages(self) -> None:
        """消费 Kafka 消息"""
        try:
            # 创建 Kafka 消费者
            # 配置优化以提高消费速度
            bootstrap_servers_list = [s.strip() for s in self.bootstrap_servers.split(",")]
            
            # 构建消费者配置
            consumer_config = {
                'bootstrap_servers': bootstrap_servers_list,
                'group_id': self.group_id,
                'enable_auto_commit': True,  # 自动提交 offset，提高性能
                'auto_commit_interval_ms': 1000,  # 1秒提交一次
                'max_poll_records': 500,  # 每次最多拉取 500 条消息
                'fetch_min_bytes': 1024,  # 最小拉取字节数
                'fetch_max_wait_ms': 100,  # 最大等待时间 100ms
                'max_partition_fetch_bytes': 10 * 1024 * 1024,  # 每个分区最多 10MB
                'value_deserializer': lambda m: m,  # 不反序列化，直接使用原始字节
                'request_timeout_ms': self.request_timeout_ms,  # 请求超时时间
                'api_version': 'auto',  # 自动检测 API 版本
            }
            
            # 添加 SASL 认证配置（如果提供）
            if self.sasl_mechanism and self.sasl_username and self.sasl_password:
                consumer_config['sasl_mechanism'] = self.sasl_mechanism
                consumer_config['sasl_plain_username'] = self.sasl_username
                consumer_config['sasl_plain_password'] = self.sasl_password
                # 使用指定的 security_protocol，如果没有指定则使用默认值
                consumer_config['security_protocol'] = self.security_protocol or 'SASL_PLAINTEXT'
            
            self.consumer = AIOKafkaConsumer(
                self.topic,
                **consumer_config
            )

            print(f"正在连接到 Kafka: {self.bootstrap_servers}")
            print(f"Topic: {self.topic}")
            print(f"Consumer Group ID: {self.group_id}")
            print(f"Bootstrap servers: {bootstrap_servers_list}")
            if self.sasl_mechanism and self.sasl_username:
                print(f"SASL 认证: {self.sasl_mechanism} (用户名: {self.sasl_username})")
                if self.security_protocol:
                    print(f"Security Protocol: {self.security_protocol}")
            
            # 尝试连接，带超时
            timeout_seconds = self.request_timeout_ms / 1000.0
            try:
                await asyncio.wait_for(self.consumer.start(), timeout=timeout_seconds)
                print("已连接到 Kafka，开始消费消息...\n")
            except asyncio.TimeoutError:
                print(f"\n错误: 连接 Kafka 超时（{timeout_seconds:.1f}秒）")
                print("可能的原因:")
                print("  1. Kafka brokers 不可达或已关闭")
                print("  2. 网络连接问题")
                print("  3. 防火墙阻止连接")
                print("  4. 需要 SSL/TLS 或 SASL 认证")
                print(f"\n请检查以下 broker 地址是否可访问:")
                for server in bootstrap_servers_list:
                    host, port = server.split(":") if ":" in server else (server, "9092")
                    print(f"  - {host}:{port}")
                raise
            except Exception as e:
                print(f"\n连接 Kafka 时出错: {e}")
                print("可能的原因:")
                print("  1. Kafka brokers 不可达或已关闭")
                print("  2. 网络连接问题")
                print("  3. 防火墙阻止连接")
                print("  4. 需要 SSL/TLS 或 SASL 认证")
                print("  5. Topic 不存在或无权限访问")
                print(f"\n请检查以下 broker 地址是否可访问:")
                for server in bootstrap_servers_list:
                    host, port = server.split(":") if ":" in server else (server, "9092")
                    print(f"  - {host}:{port}")
                raise

            # 启动 CSV 写入后台任务
            self.csv_writer_task = asyncio.create_task(self._csv_writer_worker())
            
            # 记录开始消费的时间
            self.start_consuming_time = time.time()
            print(f"开始消费，超时检测时间设置为: {self.idle_timeout_seconds}秒")
            
            # 启动超时检测任务
            self.timeout_check_task = asyncio.create_task(self._timeout_checker())

            # 将消费循环包装为任务
            async def consume_loop():
                """消费消息循环"""
                try:
                    async for message in self.consumer:
                        if not self.running:
                            break
                        # 直接处理消息，确保消费速度足够快
                        await self.process_message(message)
                except asyncio.CancelledError:
                    # 任务被取消，正常退出
                    raise
                except Exception as e:
                    # 如果是因为停止消费者导致的异常，且已经停止运行，忽略它
                    if self.running:
                        raise
                    # 否则忽略异常（可能是因为超时停止导致的）
            
            consume_task = asyncio.create_task(consume_loop())
            
            # 等待消费任务或超时检测任务完成
            done, pending = await asyncio.wait(
                [consume_task, self.timeout_check_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # 取消未完成的任务
            for task in pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        except KafkaError as e:
            print(f"Kafka 错误: {e}")
            self.stats["errors"].append({"error": f"Kafka error: {e}"})
        except Exception as e:
            print(f"消费消息时出错: {e}")
            self.stats["errors"].append({"error": str(e)})
        finally:
            # 停止超时检测任务
            if self.timeout_check_task and not self.timeout_check_task.done():
                self.timeout_check_task.cancel()
                try:
                    await self.timeout_check_task
                except asyncio.CancelledError:
                    pass
            
            # 停止消费者（如果还没有停止）
            if self.consumer:
                try:
                    await self.consumer.stop()
                except Exception:
                    pass  # 忽略停止时的错误（可能已经停止）

    def stop(self) -> None:
        """停止消费者"""
        if not self.running:
            return  # 已经停止，避免重复停止
        print("\n正在停止消费者...")
        self.running = False
        self.csv_writer_running = False
        if self.csv_writer_task and not self.csv_writer_task.done():
            self.csv_writer_task.cancel()
        if self.timeout_check_task and not self.timeout_check_task.done():
            self.timeout_check_task.cancel()

    def calculate_percentile(self, data: List[float], percentile: float) -> float:
        """计算百分位数"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile)
        if index >= len(sorted_data):
            index = len(sorted_data) - 1
        return sorted_data[index]

    def print_stats(self) -> None:
        """打印统计信息"""
        print("\n" + "=" * 80)
        print("Kafka 消费者统计信息")
        print("=" * 80)
        print(f"总消息数: {self.stats['messages_received']}")
        print(f"错误数: {len(self.stats['errors'])}")

        # 统计消息时间戳延迟
        if self.stats["message_timestamp_latency"]:
            message_timestamp_latencies = self.stats["message_timestamp_latency"]
            print(f"\n消息时间戳延迟统计 (消息时间戳到消费时间的延迟):")
            print(f"  平均延迟: {sum(message_timestamp_latencies) / len(message_timestamp_latencies):.2f} ms")
            print(f"  最小延迟: {min(message_timestamp_latencies):.2f} ms")
            print(f"  最大延迟: {max(message_timestamp_latencies):.2f} ms")
            print(f"  P50 延迟: {self.calculate_percentile(message_timestamp_latencies, 0.50):.2f} ms")
            print(f"  P90 延迟: {self.calculate_percentile(message_timestamp_latencies, 0.90):.2f} ms")
            print(f"  P95 延迟: {self.calculate_percentile(message_timestamp_latencies, 0.95):.2f} ms")
            print(f"  P99 延迟: {self.calculate_percentile(message_timestamp_latencies, 0.99):.2f} ms")
        else:
            print("\n消息时间戳延迟统计: 无数据（消息中没有时间戳）")

        # 统计事务开始延迟
        if self.stats["tx_start_latency"]:
            tx_start_latencies = self.stats["tx_start_latency"]
            print(f"\n事务开始延迟统计 (tx_start_ts 到消费时间的延迟):")
            print(f"  平均延迟: {sum(tx_start_latencies) / len(tx_start_latencies):.2f} ms")
            print(f"  最小延迟: {min(tx_start_latencies):.2f} ms")
            print(f"  最大延迟: {max(tx_start_latencies):.2f} ms")
            print(f"  P50 延迟: {self.calculate_percentile(tx_start_latencies, 0.50):.2f} ms")
            print(f"  P90 延迟: {self.calculate_percentile(tx_start_latencies, 0.90):.2f} ms")
            print(f"  P95 延迟: {self.calculate_percentile(tx_start_latencies, 0.95):.2f} ms")
            print(f"  P99 延迟: {self.calculate_percentile(tx_start_latencies, 0.99):.2f} ms")
        else:
            print("\n事务开始延迟统计: 无数据（消息 Header 中没有 tx_start_ts）")

        if self.stats["errors"]:
            print(f"\n前 10 个错误:")
            for error in self.stats["errors"][:10]:
                print(f"  {error}")

        print("=" * 80)


async def main_async(args) -> int:
    """异步主函数"""
    global _consumer_manager_instance
    
    manager = KafkaConsumerManager(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        csv_file=args.csv,
        request_timeout_ms=args.request_timeout * 1000,
        sasl_mechanism=args.sasl_mechanism,
        sasl_username=args.sasl_username,
        sasl_password=args.sasl_password,
        security_protocol=args.security_protocol,
        idle_timeout_seconds=args.idle_timeout,
    )
    _consumer_manager_instance = manager

    try:
        # 启动消费
        await manager.consume_messages()
        # 消费正常结束（可能是超时停止），等待 CSV 写入任务完成
        if manager.csv_writer_task and not manager.csv_writer_task.done():
            try:
                await asyncio.wait_for(manager.csv_writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                print("警告: CSV 写入任务未在5秒内完成")
        # 打印统计信息
        manager.print_stats()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # 用户中断或任务被取消时，执行清理
        print("\n\n收到中断信号，正在停止...")
        manager.stop()
        await asyncio.sleep(2)  # 等待任务清理
        manager.print_stats()
        return 0
    except Exception as e:
        print(f"\n错误: {e}")
        manager.stop()
        await asyncio.sleep(1)  # 等待任务清理
        manager.print_stats()
        return 1
    finally:
        _consumer_manager_instance = None

    return 0


def main():
    """主函数"""
    parser = ArgumentParser(description="Kafka 消费者程序，用于测量 Producer 到 Consumer 的时延")
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="172.23.1.43,172.23.1.44,172.23.1.45",
        help="Kafka Bootstrap Servers 地址，多个地址用逗号分隔 (默认: 172.23.1.43,172.23.1.44,172.23.1.45)",
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="要消费的 Kafka Topic",
    )
    parser.add_argument(
        "--group-id",
        default="whc-bench",
        help="Consumer Group ID",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="kafka_latency.csv",
        help="CSV 输出文件路径 (默认: kafka_latency_<timestamp>.csv)",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=30,
        help="连接超时时间（秒）(默认: 30)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=30.0,
        help="空闲超时时间（秒），如果指定时间内没有收到新消息则自动停止消费并打印统计 (默认: 10.0)",
    )
    parser.add_argument(
        "--sasl-mechanism",
        type=str,
        default='PLAIN',
        choices=['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'],
        help="SASL 认证机制 (默认: PLAIN)",
    )
    parser.add_argument(
        "--sasl-username",
        type=str,
        default='test',
        help="SASL 认证用户名 (默认: test)",
    )
    parser.add_argument(
        "--sasl-password",
        type=str,
        default='test',
        help="SASL 认证密码 (默认: test)",
    )
    parser.add_argument(
        "--security-protocol",
        type=str,
        default='SASL_PLAINTEXT',
        choices=['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'],
        help="安全协议 (默认: SASL_PLAINTEXT)",
    )

    args = parser.parse_args()

    def signal_handler(signum, frame):
        """处理 SIGINT 信号（Ctrl+C）"""
        global _consumer_manager_instance
        if _consumer_manager_instance:
            _consumer_manager_instance.stop()
        # 设置信号处理为默认行为，让 asyncio.run() 正常处理
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)

    try:
        exit_code = asyncio.run(main_async(args))
        return exit_code
    except KeyboardInterrupt:
        # 如果还有 manager 实例，打印统计信息
        global _consumer_manager_instance
        if _consumer_manager_instance:
            # 创建一个新的事件循环来运行清理代码
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(asyncio.sleep(2))
                _consumer_manager_instance.print_stats()
            finally:
                loop.close()
        return 0
    except Exception as e:
        print(f"\n错误: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

