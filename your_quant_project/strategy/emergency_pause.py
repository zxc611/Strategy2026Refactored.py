"""紧急暂停方案。"""
from __future__ import annotations


class EmergencyPauseMixin:
    def init_emergency_pause(self) -> None:
        """初始化紧急暂停方案（与策略入口保持一致）。"""
        self.install_emergency_pause_solution()

    def install_emergency_pause_solution(self) -> None:
        """安装紧急暂停解决方案：通过网络监听等方式实现暂停"""
        try:
            if getattr(self, "_emergency_pause_ready", False):
                return

            self._setup_emergency_file_watch()
            self._setup_emergency_network_listener()
            self._setup_emergency_signals()
            self._emergency_pause_ready = True
        except Exception as e:
            self._debug(f"紧急暂停初始化失败: {e}")

    def _setup_emergency_file_watch(self) -> None:
        """通过文件监控实现暂停"""
        try:
            import os
            import time
            import threading

            pause_file = os.path.join(os.getcwd(), "emergency_pause.flag")

            def check_pause_file():
                while True:
                    try:
                        if os.path.exists(pause_file):
                            if not getattr(self, "_emergency_paused", False):
                                self.pause_strategy()
                                self._emergency_paused = True
                        else:
                            if getattr(self, "_emergency_paused", False):
                                self.resume_strategy()
                                self._emergency_paused = False
                    except Exception as e:
                        self._debug(f"文件监控失败: {e}")
                    time.sleep(1)

            thread = threading.Thread(target=check_pause_file, daemon=True)
            thread.start()
        except Exception:
            pass

    def _setup_emergency_network_listener(self) -> None:
        """通过网络监听实现暂停"""
        try:
            import socket
            import threading

            def listen_for_pause():
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    sock.bind(("127.0.0.1", 9999))
                except Exception:
                    return

                while True:
                    try:
                        data, _ = sock.recvfrom(1024)
                        message = data.decode("utf-8").strip()

                        if message == "PAUSE":
                            self.pause_strategy()
                            self._emergency_paused = True
                        elif message == "RESUME":
                            self.resume_strategy()
                            self._emergency_paused = False
                        else:
                            pass
                    except Exception as e:
                        self._debug(f"网络监听失败: {e}")

            thread = threading.Thread(target=listen_for_pause, daemon=True)
            thread.start()

        except Exception:
            pass

    def _setup_emergency_signals(self) -> None:
        """设置紧急信号"""
        try:
            self._emergency_paused = False
        except Exception:
            pass

