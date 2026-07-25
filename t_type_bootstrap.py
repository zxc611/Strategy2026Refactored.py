"""t_type_bootstrap — 策略入口

策略类在模块级定义（匹配L25成功方式），
通过组合模式委托给Strategy2026实例。

关键发现：
1. 模块级 from...import strategy_2026 触发DLL崩溃（时序问题）
2. self.__class__ = _Real 动态切换类触发DLL崩溃
3. 方法绑定时super()失败（self类型不匹配）
4. __init__中创建Strategy2026实例触发DLL崩溃
5. 解决：延迟创建Strategy2026实例到on_init

根因：DLL环境下__init__阶段创建策略实例触发崩溃

FIX-20260713-LIFECYCLE-ROBUST: 生命周期方法健壮性修复
根因: pause/resume/on_destroy等显式委托方法直接调用self._real_strategy.X()
      而不检查_real_strategy是否为None。若平台在on_init完成前调用这些方法
      (或on_init失败)，_real_strategy为None导致AttributeError，操作静默失败。
修复: 所有显式委托方法添加None检查+日志，确保操作可追踪、不崩溃。
"""
from __future__ import annotations

import os
import sys
import logging
import time

_demo_dir = os.path.dirname(os.path.abspath(__file__))
if _demo_dir not in sys.path:
    sys.path.insert(0, _demo_dir)

_strategy_cache_instance = None

def register_strategy_cache(strategy_instance):
    global _strategy_cache_instance
    _strategy_cache_instance = strategy_instance

def get_strategy_cache():
    return _strategy_cache_instance


try:
    from pythongo.ui import BaseStrategy
except ImportError:
    try:
        from pythongo.base import BaseStrategy
    except ImportError:
        class BaseStrategy:
            pass


class t_type_bootstrap(BaseStrategy):
    """策略入口类 — 组合模式，延迟创建Strategy2026实例

    FIX-20260708-PARSE-FORMAT: 补充旧版CtaTemplate兼容属性
    根因：InfiniTrader C++宿主按旧版CtaTemplate格式解析策略类，
    期望找到 className / name / vtSymbol / exchange / inited / trading /
    pos / paramMap / varMap 等属性。pythongo.base.BaseStrategy 不提供
    这些属性，导致平台报"错误的解析格式"。
    修复：在类级和实例级补充这些兼容属性。
    """

    # ---- 旧版 CtaTemplate 类级属性（平台解析时读取） ----
    className = 't_type_bootstrap'   # 平台通过此属性识别策略类名
    name = ''                         # 策略实例名称
    vtSymbol = ''                     # 合约代码
    exchange = ''                     # 交易所
    inited = False                    # 是否初始化
    trading = False                   # 是否交易中
    pos = 0                           # 持仓量
    paramMap = {}                     # 参数映射表（平台UI显示用）
    varMap = {}                       # 变量映射表（平台UI显示用）

    def __init__(self, *args, **kwargs):
        BaseStrategy.__init__(self)
        self._real_strategy = None
        self._init_args = args
        self._init_kwargs = kwargs
        # FIX-PAUSE-ROOT-20260721: 已删除 __setattr__ 拦截器(基于错误假设,从未触发)
        # 原 _in_setattr_guard 初始化已移除
        # FIX-20260713-AUDIT: 标记on_init是否已被调用
        # 根因: _ensure_real_strategy在on_init前尝试创建Strategy2026实例可能触发DLL崩溃
        # (文件头注释#4: __init__中创建Strategy2026实例触发DLL崩溃)
        # 修复: 仅在on_init被调用后才允许_ensure_real_strategy延迟创建
        self._on_init_called = False
        # FIX-20260713-PLATFORM-SYNC: 存储实例引用供Strategy2026同步trading状态
        # 根因: auto_start_after_init=True时Strategy2026.on_init内部调用on_start()
        #       Strategy2026.trading=True但t_type_bootstrap.trading=False
        #       平台C++宿主读取t_type_bootstrap.trading→UI显示"初始化"
        # 修复: 在模块级存储实例引用，供Strategy2026同步trading=True
        t_type_bootstrap._instance = self

        # ---- 旧版 CtaTemplate 实例级属性（实例创建后平台读取） ----
        self.className = self.__class__.className
        self.name = ''
        self.vtSymbol = ''
        self.exchange = ''
        self.inited = False
        self.trading = False
        self.pos = 0
        self.paramMap = {}
        self.varMap = {}

        # FIX-49: 显式初始化strategy_id，等待C++平台设置真实值
        if not hasattr(self, 'strategy_id'):
            self.strategy_id = 0

    def _probe(self, method, phase, extra=None):
        """FIX-58: 生命周期探针(独立文件写入,不依赖logging模块)"""
        try:
            from infra.health_monitor import DiagnosisProbeManager as _DPM
            _DPM.on_lifecycle_call(method, phase, strategy_id=getattr(self, 'strategy_id', None),
                trading=getattr(self, 'trading', None), inited=getattr(self, 'inited', None), extra=extra)
        except Exception as _probe_err:  # FIX-20260720-12 (维度15): 异常不静默
            logging.debug("[_probe] 生命周期探针写入失败(非阻断): %s", _probe_err)

    def on_init(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[on_init] format_stack失败(非阻断): %s", _fs_err)
        logging.info("[DIAG-LIFECYCLE] on_init() CALLED thread=%s trading_before=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, _caller)
        self._probe('on_init', 'CALLED')
        self._on_init_called = True
        if self._real_strategy is None:
            from strategy.strategy_2026 import Strategy2026 as _Real
            self._real_strategy = _Real(*self._init_args, **self._init_kwargs)

        # FIX-49: strategy_id双向同步
        # 根因: t_type_bootstrap.strategy_id=0, Strategy2026.strategy_id=int(time.time())
        #       不一致→C++平台无法匹配策略实例→暂停/删除按钮无效
        try:
            _my_sid = object.__getattribute__(self, 'strategy_id')
            _real = object.__getattribute__(self, '_real_strategy')
            if _real is not None:
                _real_sid = getattr(_real, 'strategy_id', None)
                if _my_sid == 0 and _real_sid is not None and _real_sid != 0:
                    self.strategy_id = _real_sid
                    logging.info("[FIX-49] strategy_id反向同步: _real→bootstrap (值=%s)", _real_sid)
                elif _my_sid != 0:
                    _real.strategy_id = _my_sid
                    logging.info("[FIX-49] strategy_id正向同步: bootstrap→_real (值=%s)", _my_sid)
                else:
                    logging.warning("[FIX-49] strategy_id=0: 两端都为0，C++可能无法匹配策略实例")
        except Exception as _sync_err:
            logging.error("[FIX-49] strategy_id同步失败: %s", _sync_err, exc_info=True)

        # FIX-20260713-DELETE-ROOT: 注入_outer_ref引用, 让Strategy2026能找到t_type_bootstrap实例
        # 用途: 保留外层引用，供策略实例访问引导层属性(strategy_id等)
        try:
            _real._outer_ref = self
        except Exception as _pass_err_0:
            logging.debug("[on_init] 异常(非阻断): %s", _pass_err_0)

        result = self._real_strategy.on_init()
        self.inited = True

        # FIX-20260715-V5-P0-1: 彻底删除 auto_start Timer 误导性注释（V4附录I声称已实现但未落地）
        # 根因: t_type_bootstrap.py:142-153 历史注释声称"auto_start由Timer延迟0.1s执行"，
        #       但实际 strategy_2026.py:567-620 无 Timer 代码 → 注释与代码不符，
        #       导致排查方向被误导到"Timer未触发"，真实根因是"Timer已被移除"
        # 修复决策: 经用户确认不恢复 Timer，采用方案B删除误导性注释，明确告知需手动启动
        # 状态同步: 仅当 C++ 平台调用 on_start() 时才设置 trading=True
        logging.info("[t_type_bootstrap] on_init完成, inited=%s, trading=%s (auto_start已禁用，等待用户手动点击运行)",
                     self.inited, self.trading)
        import threading
        logging.info("[DIAG-LIFECYCLE] on_init() RETURN thread=%s trading=%s inited=%s",
                         threading.current_thread().name, self.trading, self.inited)
        self._probe('on_init', 'RETURNED')
        return result

    # FIX-20260713-AUDIT: on_start/on_stop/onTick/onOrder/onTrade添加None检查
    # 根因: 这些方法直接访问self._real_strategy，若on_init失败或平台在on_init前调用
    #       _real_strategy为None导致AttributeError崩溃
    # 修复: 使用_ensure_real_strategy()确保_real_strategy已创建或安全返回
    def on_start(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[on_start] format_stack失败(非阻断): %s", _fs_err)
        logging.info("[DIAG-LIFECYCLE] on_start() CALLED thread=%s trading_before=%s inited=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, self.inited, _caller)
        self._probe('on_start', 'CALLED')
        real = self._ensure_real_strategy()
        if real is None:
            return None
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): on_start 入口防御性同步 strategy_id (修复半拉子)
        # 根因: on_start 是平台启动入口，若 strategy_id 漂移则 C++ 无法匹配实例 → 后续 pause/destroy 失效。
        self._sync_strategy_id_from_real(real, 'on_start')
        result = real.on_start()
        _actual_trading = getattr(real, 'trading', False)
        if _actual_trading != self.trading:
            logging.warning("[t_type_bootstrap] on_start后trading不一致: outer=%s, real=%s, 以real为准",
                            self.trading, _actual_trading)
        self.trading = _actual_trading
        # FIX-20260715-STRATEGY-OBJ-PROBE: 记录关键状态供C++关联验证
        _outer_ref_id = id(self)
        _real_ref_id = id(real)
        _sid = getattr(self, 'strategy_id', None)
        logging.info("[DIAG-LIFECYCLE] on_start() RETURN thread=%s trading=%s result=%s strategy_id=%s outer_id=%d real_id=%d",
                         threading.current_thread().name, self.trading, result, _sid, _outer_ref_id, _real_ref_id)
        self._probe('on_start', 'RETURNED', extra={'result': result, 'outer_id': _outer_ref_id, 'real_id': _real_ref_id})
        return result

    def on_stop(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[on_stop] format_stack失败(非阻断): %s", _fs_err)
        logging.info("[DIAG-LIFECYCLE] on_stop() CALLED thread=%s trading_before=%s inited=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, self.inited, _caller)
        self._probe('on_stop', 'CALLED')
        real = self._ensure_real_strategy()
        if real is None:
            return None
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): on_stop 入口防御性同步 strategy_id (修复半拉子)
        # 根因: on_stop 是平台停止入口，若 strategy_id 漂移则 C++ 无法匹配实例 → destroy 后状态不一致。
        self._sync_strategy_id_from_real(real, 'on_stop')
        result = real.on_stop()
        # FIX-20260713-PLATFORM-STATE: on_stop后设trading=False
        # 平台C++宿主通过trading属性判断策略运行状态, on_stop后必须为False
        self.trading = False
        logging.info("[t_type_bootstrap] on_stop完成, trading=False")
        import threading
        logging.info("[DIAG-LIFECYCLE] on_stop() RETURN thread=%s trading=%s result=%s",
                         threading.current_thread().name, self.trading, result)
        self._probe('on_stop', 'RETURNED', extra={'result': result})
        return result

    def onTick(self, tick):
        # [FIX-PAUSE-OVERHEAD-REMOVE-20260722] 已彻底删除C++平台暂停按钮的onTick开销
        # 用户结论: "为次要的点击平台暂停功能增加扫描不划算"
        # 策略UI按钮暂停已100%工作，不需要C++平台按钮的兜底机制
        # 已删除: 检测层1(trading属性)、检测层2(UserLog扫描)、FIX-V2-02(状态自检)、
        #         _check_userlog_pause_signal()方法

        real = self._ensure_real_strategy()
        if real is None:
            return None
        return real.onTick(tick)

    def onOrder(self, order):
        real = self._ensure_real_strategy()
        if real is None:
            return None
        return real.onOrder(order)

    def onTrade(self, trade):
        real = self._ensure_real_strategy()
        if real is None:
            return None
        return real.onTrade(trade)

    # ---- FIX-20260710-PAUSE-DESTROY: 补全生命周期方法委托 ----
    # 根因: V3组合模式仅委托6个回调，遗漏pause/resume/destroy等生命周期方法
    # 导致平台C++宿主调用pause()/on_destroy()时命中BaseStrategy空操作
    # 全链条: C++宿主 → t_type_bootstrap.pause() → _real_strategy.pause()
    #   → strategy_core.pause() → LifecycleCallbacks.pause() → 四元状态同步
    #
    # FIX-20260713-LIFECYCLE-ROBUST: 添加None检查+入口日志
    # 根因: 若平台在on_init完成前调用pause/resume/destroy，_real_strategy为None
    #       导致AttributeError，操作静默失败，用户看到"暂停/删除无效"
    # 修复: 所有方法检查_real_strategy是否为None，记录入口日志便于追踪

    def _ensure_real_strategy(self):
        """确保_real_strategy已创建，若为None则尝试延迟创建。返回_real_strategy或None。

        FIX-20260713-AUDIT: 仅在on_init被调用后才尝试延迟创建
        根因: 文件头注释#4 "__init__中创建Strategy2026实例触发DLL崩溃"
              若on_init前平台调用pause/resume等，_ensure_real_strategy尝试创建
              可能触发DLL级崩溃(非Python异常，try/except无法捕获)
        修复: 检查_on_init_called标志，on_init前直接返回None
        """
        real = object.__getattribute__(self, '_real_strategy')
        if real is None:
            # FIX-20260713-AUDIT: on_init前不尝试延迟创建，防止DLL崩溃
            if not object.__getattribute__(self, '_on_init_called'):
                logging.warning("[t_type_bootstrap] _real_strategy为None且on_init尚未调用，跳过延迟创建(防止DLL崩溃)")
                return None
            logging.warning("[t_type_bootstrap] _real_strategy为None(on_init已调用)，尝试延迟创建")
            try:
                from strategy.strategy_2026 import Strategy2026 as _Real
                real = _Real(*self._init_args, **self._init_kwargs)
                self._real_strategy = real
                # FIX-20260713-STRATEGY-ID-SYNC: 延迟创建时也同步strategy_id
                try:
                    _my_sid = object.__getattribute__(self, 'strategy_id')
                    if getattr(real, 'strategy_id', 0) == 0:
                        real.strategy_id = _my_sid
                except Exception:
                    pass
                logging.info("[t_type_bootstrap] 延迟创建_real_strategy成功")
            except Exception as _create_err:
                logging.error("[t_type_bootstrap] 延迟创建_real_strategy失败: %s", _create_err, exc_info=True)
                return None
        return real

    def _sync_strategy_id_from_real(self, real, method_name):
        """FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): 防御性同步 strategy_id 的统一辅助方法

        根因: FIX-F5 仅在 pause/resume 入口添加防御性 strategy_id 同步，但 t_type_bootstrap 共有
              9 个委托方法 (pause/resume/on_pause/on_resume/on_stop/on_destroy/on_start/stop/destroy)，
              均通过 real.X() 委托到 _real_strategy。若实盘出现 strategy_id 漂移（极端场景如热重载/
              平台重新分配），未做同步的方法会带着旧 strategy_id 委托 → C++ 平台无法匹配实例。
              这是 FIX-F5 半拉子工程 — 仅修了 2 个方法，遗漏 7 个。
        修复: 抽取统一辅助方法 _sync_strategy_id_from_real(real, method_name)，在所有委托方法
              入口（_ensure_real_strategy + None 检查之后）调用，确保 9 个方法对称同步。
        设计: 以 real.strategy_id 为权威值回写到 bootstrap (与 on_init L138-139 一致)；real.strategy_id
              为 None 时不回写 (避免 0 覆盖已有有效值)；使用 object.__getattribute__ 避免 __getattr__ 递归。
        """
        try:
            _real_sid = getattr(real, 'strategy_id', None)
            _my_sid = object.__getattribute__(self, 'strategy_id')
            if _real_sid is not None and _real_sid != _my_sid:
                self.strategy_id = _real_sid
                logging.info("[FIX-F5-EXT] %s: strategy_id 防御性同步 real→bootstrap (值=%s)", method_name, _real_sid)
        except Exception as _sid_sync_err:
            logging.debug("[FIX-F5-EXT] %s: strategy_id 同步失败(非阻断): %s", method_name, _sid_sync_err)

    def pause(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[pause] format_stack失败(非阻断): %s", _fs_err)
        logging.info("[DIAG-LIFECYCLE] pause() CALLED thread=%s trading=%s inited=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, self.inited, _caller)
        self._probe('pause', 'CALLED')
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] pause(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): 统一调用辅助方法 (替代原 inline 同步块)
        self._sync_strategy_id_from_real(real, 'pause')
        result = real.pause()
        # FIX-20260714-R14: pause()同步trading=False到引导层
        # 根因R14: pause()未同步self.trading=False，C++读取trading仍为True→UI显示"运行中"
        # 修复: 与on_start/on_stop/stop/destroy保持一致，委托方法中同步trading状态
        self.trading = False
        logging.info("[t_type_bootstrap] pause完成, trading=False")
        self._probe('pause', 'RETURNED', extra={'result': result})
        return result

    def on_pause(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[on_pause] format_stack失败(非阻断): %s", _fs_err)
        # FIX-V2-03: 增强探针(与on_stop/on_destroy对齐，记录caller_stack+trading状态)
        # 根因: on_pause原仅记录"CALLED from platform"无caller_stack，无法定位C++调用链路
        # 修复: 补全thread/trading/caller_stack，与on_stop(L235)/on_destroy(L437)格式统一
        logging.info("[DIAG-LIFECYCLE] on_pause() CALLED thread=%s trading=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, _caller)
        self._probe('on_pause', 'CALLED')
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] on_pause(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): on_pause 入口防御性同步 strategy_id (修复半拉子)
        # 根因: 平台 CamelCase 路径 onPause→on_pause 走此方法，若漂移未同步则 C++ 无法匹配实例。
        self._sync_strategy_id_from_real(real, 'on_pause')
        result = real.on_pause()
        self._probe('on_pause', 'RETURNED', extra={'result': result})
        return result

    def on_destroy(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[on_destroy] format_stack失败(非阻断): %s", _fs_err)
        logging.info("[DIAG-LIFECYCLE] on_destroy() CALLED thread=%s trading=%s inited=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, self.inited, _caller)
        self._probe('on_destroy', 'CALLED')
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] on_destroy(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): on_destroy 入口防御性同步 strategy_id (修复半拉子)
        # 根因: 平台 CamelCase 路径 onDestroy→on_destroy 走此方法，若漂移未同步则 C++ 无法匹配实例。
        self._sync_strategy_id_from_real(real, 'on_destroy')
        result = real.on_destroy()
        # FIX-20260713-PLATFORM-STATE: on_destroy后重置平台状态属性
        self.inited = False
        self.trading = False
        logging.info("[t_type_bootstrap] on_destroy完成, inited=False, trading=False")
        self._probe('on_destroy', 'RETURNED', extra={'result': result})
        return result

    def internal_pause_strategy(self):
        real = self._ensure_real_strategy()
        if real is None:
            return False
        return real.internal_pause_strategy()

    def internal_resume_strategy(self):
        real = self._ensure_real_strategy()
        if real is None:
            return False
        return real.internal_resume_strategy()

    def resume(self):
        logging.info("[FIX-20260713-LIFECYCLE] resume() CALLED from platform")
        self._probe('resume', 'CALLED')
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] resume(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): 统一调用辅助方法 (替代原 inline 同步块)
        self._sync_strategy_id_from_real(real, 'resume')
        result = real.resume()
        # FIX-20260714-R15: resume()同步trading=True到引导层
        # 根因R15: resume()未同步self.trading=True，C++读取trading仍为False→UI显示"暂停"
        # 修复: 与on_start/on_stop/pause保持一致，委托方法中同步trading状态
        self.trading = True
        logging.info("[t_type_bootstrap] resume完成, trading=True")
        self._probe('resume', 'RETURNED', extra={'result': result})
        return result

    def on_resume(self):
        import threading
        import traceback
        try:
            _caller = ''.join(traceback.format_stack()[-4:-1])
        except Exception as _fs_err:
            _caller = ""
            logging.debug("[on_resume] format_stack失败(非阻断): %s", _fs_err)
        # FIX-V2-03: 增强探针(与on_stop/on_destroy/on_pause对齐，记录caller_stack+trading状态)
        logging.info("[DIAG-LIFECYCLE] on_resume() CALLED thread=%s trading=%s caller_stack:\n%s",
                         threading.current_thread().name, self.trading, _caller)
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] on_resume(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): on_resume 入口防御性同步 strategy_id (修复半拉子)
        # 根因: 平台 CamelCase 路径 onResume→on_resume 走此方法，若漂移未同步则 C++ 无法匹配实例。
        self._sync_strategy_id_from_real(real, 'on_resume')
        return real.on_resume()

    def stop(self):
        logging.info("[FIX-20260713-LIFECYCLE] stop() CALLED from platform")
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] stop(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): stop 入口防御性同步 strategy_id (修复半拉子)
        # 根因: stop 是平台蛇形回调路径，与 on_stop 别名关系但若漂移未同步则 C++ 无法匹配实例。
        self._sync_strategy_id_from_real(real, 'stop')
        result = real.on_stop()
        self.trading = False
        logging.info("[t_type_bootstrap] stop完成, trading=False")
        return result

    def destroy(self):
        logging.info("[FIX-20260713-LIFECYCLE] destroy() CALLED from platform")
        real = self._ensure_real_strategy()
        if real is None:
            logging.error("[t_type_bootstrap] destroy(): _real_strategy为None，操作被跳过")
            return False
        # FIX-F5-EXT (NEW-R7 衍生, 2026-07-19): destroy 入口防御性同步 strategy_id (修复半拉子)
        # 根因: destroy 是平台蛇形回调路径，与 on_destroy 别名关系但若漂移未同步则 C++ 无法匹配实例。
        self._sync_strategy_id_from_real(real, 'destroy')
        result = real.onDestroy()
        self.inited = False
        self.trading = False
        logging.info("[t_type_bootstrap] destroy完成, inited=False, trading=False")
        return result

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        real = object.__getattribute__(self, '_real_strategy')
        if real is not None and hasattr(real, name):
            return getattr(real, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    # ---- 别名 ----
    onInit = on_init
    onStart = on_start
    onStop = on_stop
    on_tick = onTick
    on_order = onOrder
    on_trade = onTrade
    onPause = on_pause
    onResume = on_resume
    onDestroy = on_destroy  # FIX-20260713-DELETE: 缺失onDestroy别名导致平台CamelCase调用绕过flag重置
    onStopAlias = on_stop  # 平台可能使用不同大小写

    # FIX-20260720-4 (RC-20260720-4): 补全首字母大写别名（无 on 前缀）
    # 根因: C++ 平台可能通过 PyObject_GetAttrString(obj, "Pause"/"Stop"/"Destroy"/"Resume")
    #       查找回调方法，若未定义则属性查找失败 → C++ 不调用 Python → 暂停/删除无效
    # 证据: lifecycle_probe.jsonl 2026-07-20 ZERO pause/on_stop/on_destroy 事件
    #       UserLog.txt 10:18:40-54 用户点击4次Pause但Python未收到任何回调
    Pause = pause
    Resume = resume
    Stop = stop
    Destroy = destroy


Strategy2026 = t_type_bootstrap

__all__ = ['t_type_bootstrap', 'Strategy2026', 'register_strategy_cache', 'get_strategy_cache']
__version__ = '2.9.0'
