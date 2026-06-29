# [M1-79] 日志工具
# MODULE_ID: M1-092

"""logging_utils _统一日志工厂 (R9-5)



提供 ``get_logger`` 统一入口，替代分散在代码各处理

``logging.getLogger(__name__)`` 模式式



优势:

- 单点控制: 未来切换日志框架/格式只需修改此模块

- 一致） 所。logger 通过同一入口创建

- 可扩展 可注入模块上下文、默所handler _

"""



from __future__ import annotations



import logging

import sys

from typing import Optional



__all__ = ['get_logger']



# 是否已安装默所handler（避免重复添加）'
_default_handler_installed = False





def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:

    """获取统一 logger 实例例



    Args:

        name: logger 名称，通常册``__name__``_

        level: 可选日志级别，_``logging.DEBUG``_

               不传时使。logger 自身。root 的默认级别名



    Returns:

        配置好的 ``logging.Logger`` 实例例

    """

    logger = logging.getLogger(name)

    if level is not None:

        logger.setLevel(level)

    return logger

