"""
Tushare 数据获取模块（已适配第三方代理接口 http://118.89.66.41:8010/）
完全对齐参考代码，确保代理生效
"""
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts

from .base import BaseDataFetcher, canonical_stock_code

logger = logging.getLogger(__name__)


class TushareFetcher(BaseDataFetcher):
    """Tushare 数据源接入类（第三方代理版，无需Tushare积分/权限）"""

    def __init__(self):
        super().__init__()
        # 从环境变量读取Token（按参考代码格式）
        self.token = os.getenv("TUSHARE_TOKEN", "IqRuUKOMrtwRjiiQquHcrbHOtMnsugtMHYspfHZxpPFSGrvCSiWlVRqHsTCyOfd")
        self.pro = None
        self._init_api()

    def _init_api(self):
        """初始化 Tushare API（严格对齐参考代码，强制代理地址）"""
        try:
            # 1. 按参考代码：直接用token初始化pro_api
            self.pro = ts.pro_api(self.token)
            
            # 2. 核心修改：强制覆盖API地址为代理地址（严格对齐截图代码）
            self.pro._DataApi__http_url = "http://118.89.66.41:8010/"
            
            logger.info("✅ Tushare 代理接口初始化成功")
            logger.info("   代理地址: http://118.89.66.41:8010/")
            logger.info("   Token: 已配置")

        except Exception as e:
            logger.error(f"❌ Tushare 代理接口初始化失败: {e}")
            self.pro = None

    def _format_ts_code(self, code: str) -> str:
        """统一股票代码格式（603778 -> 603778.SH，符合Tushare要求）"""
        code = canonical_stock_code(code)
        if code.startswith("6"):
            return f"{code}.SH"
        elif code.startswith("0") or code.startswith("3"):
            return f"{code}.SZ"
        return code

    def fetch_daily(self, code, start_date=None, end_date=None):
        """
        获取日线数据（完全兼容原项目逻辑）
        :param code: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: DataFrame
        """
        if not self.pro:
            logger.warning("Tushare 代理API未初始化，跳过该数据源")
            return None

        try:
            ts_code = self._format_ts_code(code)
            logger.info(f"[Tushare代理] 开始获取 {ts_code} 日线数据...")

            # 默认时间范围（近30天）
            if not end_date:
                end_date = datetime.now().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

            # 调用Tushare接口（和官方完全一致，仅代理地址生效）
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                logger.warning(f"[Tushare代理] {ts_code} 返回空数据")
                return None

            # 按日期降序排列，适配项目后续分析
            df = df.sort_values("trade_date", ascending=False).reset_index(drop=True)
            logger.info(f"[Tushare代理] {ts_code} 数据获取成功，共 {len(df)} 条")
            return df

        except Exception as e:
            logger.error(f"[Tushare代理] {ts_code} 数据获取失败: {e}")
            return None

    def fetch_stock_name(self, code):
        """获取股票名称（适配原项目的名称获取逻辑）"""
        if not self.pro:
            return None

        try:
            ts_code = self._format_ts_code(code)
            df = self.pro.stock_basic(ts_code=ts_code, fields="ts_code,name")
            if not df.empty:
                return df.iloc[0]["name"]
            return None
        except Exception as e:
            logger.warning(f"[Tushare代理] 获取 {ts_code} 名称失败: {e}")
            return None
