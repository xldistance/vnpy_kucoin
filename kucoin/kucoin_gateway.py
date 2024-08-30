import base64
import csv
import hashlib
import hmac
import json
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta, timezone
from enum import Enum
from inspect import signature
from pathlib import Path
from threading import Lock
from time import sleep, time
from typing import Any, Dict, List
from urllib.parse import urlencode

from peewee import chunked
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event
from vnpy.event.engine import EventEngine
from vnpy.trader.constant import Direction, Exchange, Interval, Offset, Status
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    OrderType,
    PositionData,
    Product,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import kucoin_account  # 导入账户字典
from vnpy.trader.utility import (
    TZ_INFO,
    GetFilePath,
    delete_dr_data,
    extract_vt_symbol,
    get_symbol_mark,
    get_local_datetime,
    get_uuid,
    is_target_contract,
    load_json,
    remain_alpha,
    remain_digit,
    save_connection_status,
    save_json,
)

recording_list = GetFilePath.recording_list

# REST API地址
REST_HOST: str = "https://api-futures.kucoin.com"

# Websocket API地址
WEBSOCKET_HOST: str = "wss://ws-api-futures.kucoin.com"

# 委托类型映射
ORDERTYPE_VT2KUCOIN = {OrderType.LIMIT: "limit", OrderType.MARKET: "market"}

ORDERTYPE_KUCOIN2VT = {v: k for k, v in ORDERTYPE_VT2KUCOIN.items()}

# 买卖方向映射
DIRECTION_VT2KUCOIN = {
    Direction.LONG: "buy",
    Direction.SHORT: "sell",
}
DIRECTION_KUCOIN2VT = {v: k for k, v in DIRECTION_VT2KUCOIN.items()}

# 多空反向映射
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}


# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
# 合约数据缓存
CONTRACT_DATA = {}
# ----------------------------------------------------------------------------------------------------
class KucoinGateway(BaseGateway):
    """vn.py用于对接KUCOIN的交易接口"""

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "host": "",
        "port": 0,
    }

    exchanges: Exchange = [Exchange.KUCOIN]
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine: EventEngine, gateway_name: str = "KUCOIN") -> None:
        """
        构造函数
        """
        super().__init__(event_engine, gateway_name)

        self.ws_api: "KucoinWebsocketApi" = KucoinWebsocketApi(self)
        self.rest_api: "KucoinRestApi" = KucoinRestApi(self)
        self.orders: Dict[str, OrderData] = {}
        self.recording_list = [vt_symbol for vt_symbol in recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 查询历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        self.query_contracts = [vt_symbol for vt_symbol in GetFilePath.all_trading_vt_symbols if is_target_contract(vt_symbol, self.gateway_name)]
        self.query_functions = [self.query_account, self.query_order, self.query_position]
        # 查询历史数据状态
        self.history_status = True
        # 订阅逐笔成交数据状态
        self.book_trade_status: bool = False
    # ----------------------------------------------------------------------------------------------------
    def connect(self, log_account: dict = {}) -> None:
        """
        连接交易接口
        """
        if not log_account:
            log_account = kucoin_account
        key: str = log_account["key"]
        secret: str = log_account["secret"]
        proxy_host: str = log_account["host"]
        proxy_port: int = log_account["port"]
        passphrase = log_account["passphrase"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, proxy_host, proxy_port, passphrase)
        self.ws_api.connect(key, secret, proxy_host, proxy_port, passphrase)
        self.init_query()
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        self.ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        """
        查询持仓
        """
        self.rest_api.query_position()
    # ----------------------------------------------------------------------------------------------------
    def query_order(self) -> None:
        """
        查询活动委托单
        """
        self.rest_api.query_order()
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        推送委托数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def get_order(self, orderid: str) -> OrderData:
        """
        查询委托数据
        """
        return self.orders.get(orderid, None)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, event: Event):
        """
        查询合约历史数据
        """
        if len(self.history_contracts) > 0:
            symbol, exchange, gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.MINUTE,
                start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
                end=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.rest_api.query_history(req)
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event) -> None:
        """
        处理定时事件
        """
        function = self.query_functions.pop(0)
        function()
        self.query_functions.append(function)

        if self.query_contracts:
            vt_symbol = self.query_contracts.pop(0)
            symbol, exchange, gateway_name = extract_vt_symbol(vt_symbol)
            self.rest_api.query_tick(symbol)
            self.query_contracts.append(vt_symbol)
    # ----------------------------------------------------------------------------------------------------
    def init_query(self):
        """ """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
    # ----------------------------------------------------------------------------------------------------
    def close(self) -> None:
        """
        关闭连接
        """
        self.rest_api.stop()
        self.ws_api.stop()
# ----------------------------------------------------------------------------------------------------
class KucoinRestApi(RestClient):
    """
    KUCOIN交易所REST API
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, gateway: KucoinGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ws_api: KucoinWebsocketApi = self.gateway.ws_api

        # 保存用户登陆信息
        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""
        # 生成委托单号加线程锁
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
        self.ticks: Dict[str, TickData] = self.gateway.ws_api.ticks
        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}
        # 账户查询币种
        self.currencies = ["XBT", "USDT"]
        # rest api令牌用于websocket api连接令牌
        self.token = ""
        # 用户自定义委托单id和交易所委托单id映射
        self.orderid_map: Dict[str, str] = defaultdict(str)
    # ----------------------------------------------------------------------------------------------------
    def sign(self, request: Request) -> Request:
        """
        生成KUCOIN签名
        """
        # 获取鉴权类型并将其从data中删除
        security = request.data.pop("security")
        if security == Security.NONE:
            request.data = None
            return request

        nonce = int(time() * 1000)
        method = request.method
        params = request.params
        uri_path = request.path
        data_json = ""
        if method in ["GET", "DELETE"]:
            if params:
                uri_path: str = request.path + "?" + urlencode(params)
        else:
            if request.data:
                data_json = json.dumps(request.data)
                request.data = data_json
            uri_path = request.path + data_json
        str_to_sign = str(nonce) + method + uri_path
        sign = self.generate_signature(str_to_sign)
        passphrase = self.generate_signature(self.passphrase)
        if not request.headers:
            request.headers = {"Content-Type": "application/json"}

        request.headers.update(
            {"KC-API-KEY": self.key, "KC-API-SIGN": sign, "KC-API-TIMESTAMP": str(nonce), "KC-API-PASSPHRASE": passphrase, "KC-API-KEY-VERSION": "3"}
        )
        return request
    # ----------------------------------------------------------------------------------------------------
    def generate_signature(self, msg: str):
        return base64.b64encode(hmac.new(self.secret, msg.encode("utf-8"), hashlib.sha256).digest()).decode()
    # ----------------------------------------------------------------------------------------------------
    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int,
        passphrase: str,
    ) -> None:
        """
        连接REST服务器
        """
        self.key = key
        self.secret = secret.encode()
        self.passphrase = passphrase
        self.connect_time = int(datetime.now().strftime("%Y%m%d%H%M%S"))
        self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")
        self.query_contract()
    # ----------------------------------------------------------------------------------------------------
    def get_token(self, is_private: bool = True):
        """
        * 获取websocket私有和公共令牌。
        * 参数:
            is_private (bool): 是否需要获取私有令牌（默认为True）
        """
        if is_private:
            data: dict = {"security": Security.SIGNED}
            path: str = "/api/v1/bullet-private"
        else:
            data: dict = {"security": Security.NONE}
            path: str = "/api/v1/bullet-public"
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_token,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def on_token(self, data: dict, request: Request):
        """
        收到token回报
        """
        self.token: str = data["data"]["token"]
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        path: str = "/api/v1/account-overview"
        """
        currency = self.currencies.pop(0)
        params = {"currency": currency}
        self.add_request(method="GET", path=path, callback=self.on_query_account, data=data, params=params)
        self.currencies.append(currency)
        """
        for currency in self.currencies:
            params = {"currency": currency}
            self.add_request(method="GET", path=path, callback=self.on_query_account, data={"security": Security.SIGNED}, params=params)
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        """
        查询持仓
        """
        data: dict = {"security": Security.SIGNED}
        path: str = "/api/v1/positions"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data,
        )
    # ----------------------------------------------------------------------------------------------------
    def query_order(self) -> None:
        """
        查询活动委托单
        """
        data: dict = {"security": Security.SIGNED}
        params = {"status": "active"}
        path: str = "/api/v1/orders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data,
            params=params,
        )
    # ----------------------------------------------------------------------------------------------------
    def query_contract(self) -> None:
        """
        查询合约信息
        """
        data: dict = {"security": Security.NONE}
        path: str = "/api/v1/contracts/active"

        self.add_request(method="GET", path=path, callback=self.on_query_contract, data=data)
    # ----------------------------------------------------------------------------------------------------
    def query_tick(self, symbol: str) -> None:
        """
        查询合约市场行情
        """
        data: dict = {"security": Security.NONE}
        path: str = f"/api/v1/contracts/{symbol}"
        self.add_request(method="GET", path=path, callback=self.on_query_tick, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_query_tick(self, data: dict, request: Request) -> None:
        """
        收到市场行情回报
        """
        raw = data["data"]
        symbol = raw["symbol"]
        tick = self.ticks.get(symbol,None)
        if not tick:
            return
        # 反向合约币的成交量
        if raw["isInverse"]:
            tick.volume = raw["turnoverOf24h"]
        else:
            # U本位合约币的成交量
            tick.volume = raw["volumeOf24h"]
        tick.high_price = raw["highPrice"]
        tick.low_price = raw["lowPrice"]
        tick.open_interest = raw["openInterest"]
    # ----------------------------------------------------------------------------------------------------
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        # 生成本地委托号
        orderid: str = req.symbol + "-" + str(self.connect_time + self._new_order_id())

        # 推送提交中事件
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        data: dict = {
            "security": Security.SIGNED,
            "symbol": req.symbol,
            "side": DIRECTION_VT2KUCOIN[req.direction],
            "price": str(req.price),
            "size": int(req.volume),
            "type": ORDERTYPE_VT2KUCOIN[req.type],
            "clientOid": orderid,
            "leverage": str(20),
        }
        if req.offset == Offset.CLOSE:
            data["reduceOnly"] = True
        else:
            data["reduceOnly"] = False
        self.add_request(
            method="POST",
            path="/api/v1/orders",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )
        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        必须用api生成的订单编号撤单
        """
        data: dict = {"security": Security.SIGNED}
        gateway_id = self.orderid_map[req.orderid]
        if not gateway_id:
            if self.orderid_map:
                local_id = list(self.orderid_map)[0]
                gateway_id = self.orderid_map[local_id]
                self.orderid_map.pop(local_id)
                self.gateway.write_log(f"合约：{req.vt_symbol}未获取到委托单id映射：自定义委托单id：{req.orderid}，使用交易所orderid：{gateway_id}撤单")

        path: str = "/api/v1/orders/" + gateway_id
        order: OrderData = self.gateway.get_order(req.orderid)
        self.add_request(method="DELETE", path=path, callback=self.on_cancel_order, data=data, on_failed=self.on_cancel_failed, extra=order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        资金查询回报
        """
        asset = data["data"]
        account: AccountData = AccountData(
            accountid=asset["currency"] + "_" + self.gateway_name,
            balance=asset["accountEquity"],
            available=asset["availableBalance"],
            position_profit=asset["unrealisedPNL"],
            datetime=datetime.now(TZ_INFO),
            file_name=self.gateway.account_file_name,
            gateway_name=self.gateway_name,
        )
        account.frozen = account.balance - account.available
        if account.balance:
            self.gateway.on_account(account)
            # 保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__

        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = str(GetFilePath.ctp_account_path).replace("ctp_account_main", self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)
    # ----------------------------------------------------------------------------------------------------
    def on_query_position(self, data: dict, request: Request) -> None:
        """
        持仓查询回报
        """
        for raw in data["data"]:
            volume = float(raw["currentQty"])
            if volume >= 0:
                direction = Direction.LONG
            elif volume < 0:
                direction = Direction.SHORT
            position_1: PositionData = PositionData(
                symbol=raw["symbol"],
                exchange=Exchange.KUCOIN,
                direction=direction,
                volume=abs(volume),
                price=float(raw["avgEntryPrice"]) if raw["avgEntryPrice"] else 0,
                pnl=float(raw["unrealisedPnl"]),
                gateway_name=self.gateway_name,
            )
            position_2 = PositionData(
                symbol=raw["symbol"],
                exchange=Exchange.KUCOIN,
                gateway_name=self.gateway_name,
                direction=OPPOSITE_DIRECTION[position_1.direction],
                volume=0,
                price=0,
                pnl=0,
            )
            self.gateway.on_position(position_1)
            self.gateway.on_position(position_2)
    # ----------------------------------------------------------------------------------------------------
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        委托查询回报
        """
        if not data["data"]["items"]:
            return
        for raw in data["data"]["items"]:
            volume = float(raw["size"])
            trade_volume = float(raw["filledSize"])
            if raw["status"] == "done":
                if volume == trade_volume:
                    status = Status.ALLTRADED
                if float(raw["canceledSize"]):
                    status = Status.CANCELLED
            elif raw["status"] == "open":
                status = Status.NOTTRADED
            elif raw["status"] == "match":
                if volume == trade_volume:
                    status = Status.ALLTRADED
                elif volume > trade_volume:
                    status = Status.PARTTRADED

            order: OrderData = OrderData(
                orderid=raw["clientOid"] if raw["clientOid"] else raw["id"],
                symbol=raw["symbol"],
                exchange=Exchange.KUCOIN,
                price=float(raw["price"]),
                volume=volume,
                type=ORDERTYPE_KUCOIN2VT[raw["type"]],
                direction=DIRECTION_KUCOIN2VT[raw["side"]],
                traded=trade_volume,
                status=status,
                datetime=get_local_datetime(raw["createdAt"]),
                gateway_name=self.gateway_name,
            )
            self.orderid_map[order.orderid] = raw["id"]
            if raw["reduceOnly"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def on_query_contract(self, data: dict, request: Request):
        """
        合约信息查询回报
        """
        for raw in data["data"]:
            contract: ContractData = ContractData(
                symbol=raw["symbol"],
                exchange=Exchange.KUCOIN,
                name=raw["symbol"],
                price_tick=raw["tickSize"],
                size=raw["multiplier"],
                max_volume=raw["maxOrderQty"],
                min_volume=raw["lotSize"],
                open_commission_ratio=raw["takerFeeRate"],
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            # 过滤过期交割合约
            if contract.symbol[-1].isdigit() and raw["expireDate"]:
                expire_date = get_local_datetime(raw["expireDate"])
                contract_postfix = expire_date.strftime("%Y%m%d")
                current_postfix = datetime.now().strftime("%Y%m%d")
                if int(contract_postfix) <= int(current_postfix):
                    continue
            CONTRACT_DATA[contract.symbol] = contract
            self.gateway.on_contract(contract)
        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        委托下单回报
        """
        code = int(data["code"])
        if code // 100 != 2 :
            msg = data["msg"]
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            self.gateway.write_log(f"合约：{order.vt_symbol}委托失败，状态码：{code}，信息：{msg}")
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        委托下单回报函数报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        委托下单失败服务器报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        msg: str = "委托失败，状态码：{0}，信息：{1}".format(status_code, request.response.text)
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, status_code: str, request: Request) -> None:
        """
        委托撤单回报
        """
        data = request.response.json()
        code = data["code"]
        if int(code) == 100004:
            msg = data["msg"]
            if request.extra:
                order = request.extra
                order.status = Status.REJECTED
                self.gateway.on_order(order)
            msg = f"撤单失败，状态码：{code}，信息：{msg}"
            self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_failed(self, status_code: str, request: Request):
        """
        撤单回报函数报错回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        # 获取合约数据完成再下载历史数据
        while not CONTRACT_DATA:
            sleep(1)

        history = []
        limit = 200
        start_time = req.start
        time_consuming_start = time()
        # 已经获取了所有可用的历史数据或者start已经到了请求的终止时间则终止循环
        # kucoin有的合约历史数据不足limit条，所以不用判断len(buf) < limit
        while start_time < req.end:
            end_time = start_time + timedelta(minutes=limit)
            params = {
                "symbol": req.symbol,
                "granularity": 1,
                "from": int(start_time.timestamp() * 1000),
                "to": int(end_time.timestamp() * 1000)
            }
            resp = self.request("GET", "/api/v1/kline/query", data={"security": Security.NONE}, params=params)

            if not resp or resp.status_code != 200:
                msg = f"标的：{req.vt_symbol}获取历史数据失败，状态码：{getattr(resp, 'status_code', '无响应')}, 信息：{getattr(resp, 'text', '')}"
                self.gateway.write_log(msg)
                break

            data = resp.json()
            if "data" not in data:
                delete_dr_data(req.symbol, self.gateway_name)
                msg = f"标的：{req.vt_symbol}获取历史数据为空，收到数据：{data}"
                self.gateway.write_log(msg)
                break

            buf = []
            for raw_data in data["data"]:
                volume = raw_data[5] * CONTRACT_DATA[req.symbol].size

                bar = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=get_local_datetime(raw_data[0]),
                    interval=req.interval,
                    open_price=raw_data[1],
                    high_price=raw_data[2],
                    low_price=raw_data[3],
                    close_price=raw_data[4],
                    volume=volume,
                    gateway_name=self.gateway_name,
                )
                buf.append(bar)

            history.extend(buf)
            start_time = end_time

        if history:
            try:
                database_manager.save_bar_data(history, False)
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return

            time_consuming_end = time()
            query_time = round(time_consuming_end - time_consuming_start, 3)
            msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime}，结束时间：{history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
            self.gateway.write_log(msg)
        else:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
# ----------------------------------------------------------------------------------------------------
class KucoinWebsocketApi(WebsocketClient):
    """
    KUCOIN交易所Websocket接口
    """
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, gateway: KucoinGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway: KucoinGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        # 成交委托号
        self.trade_id: int = 0
        self.ws_connected: bool = False
        self.ping_count: int = 0
        self.gateway.event_engine.register(EVENT_TIMER, self.send_ping)
        self.func_map = {
            "tickerV2":self.on_bbo,
            "match": self.on_public_trade,
            "level2": self.on_depth,
            "symbolOrderChange": self.on_order,
            "position.change": self.on_position
            }
    # ----------------------------------------------------------------------------------------------------
    def send_ping(self, event):
        """
        发送ping
        """
        self.ping_count += 1
        if self.ping_count < 10:
            return
        self.ping_count = 0
        self.send_packet({"id": get_uuid(), "type": "ping"})
    # ----------------------------------------------------------------------------------------------------
    def get_ws_host(self):
        """
        生成WEBSOCKET API连接地址
        """
        token = self.gateway.rest_api.token
        while not token:
            self.gateway.rest_api.get_token(True)
            token = self.gateway.rest_api.token
            sleep(1)

        ws_host = f"{WEBSOCKET_HOST}?token={token}&connectId={get_uuid()}"
        return ws_host
    # ----------------------------------------------------------------------------------------------------
    def connect(self, api_key: str, api_secret: str, proxy_host: str, proxy_port: int, passphrase: str) -> None:
        """
        连接Websocket交易频道
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase

        ws_host = self.get_ws_host()
        self.init(ws_host, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """
        连接成功回报
        """
        self.ws_connected = True
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接成功")

        for req in list(self.subscribed.values()):
            self.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self) -> None:
        """
        连接断开回报
        """
        self.ws_connected = False
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接断开")
        self.host = self.get_ws_host()
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        # 等待ws连接成功后再订阅行情
        while not self.ws_connected:
            sleep(1)
        self.ticks[req.symbol] = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=req.exchange,
            gateway_name=self.gateway_name,
            datetime=datetime.now(TZ_INFO),
        )

        self.subscribed[req.symbol] = req
        self.subscribe_topics(req)
    # ----------------------------------------------------------------------------------------------------    
    def subscribe_topics(self, req:SubscribeRequest):
        """
        订阅公共和私有主题
        """
        topics = [
            f"/contractMarket/level2Depth5:{req.symbol}",  # 订阅5档深度
            f"/contractMarket/execution:{req.symbol}",       # 订阅逐笔成交
        ]
        if self.gateway.book_trade_status:
            topics.append(f"/contractMarket/tickerV2:{req.symbol}")  # 逐笔一档深度(占用大量带宽)

        private_topics = [
            f"/contractMarket/tradeOrders:{req.symbol}",      # 私有主题：交易订单
            f"/contract/position:{req.symbol}"                # 私有主题：持仓信息
        ]

        for topic in topics:
            self.send_packet({"id": get_uuid(), "type": "subscribe", "topic": topic, "response": True})

        for topic in private_topics:
            self.send_packet({"type": "subscribe", "topic": topic, "response": True, "privateChannel": True})
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Any) -> None:
        """
        推送数据回报
        """
        # 更新推送
        type_ = packet["type"]
        # 过滤心跳数据和触发订阅推送
        if type_ in ["pong", "welcome", "ack"]:
            return
        if type_ == "error":
            msg = packet["data"]
            # token过期重启交易接口
            if msg == "token is expired":
                save_connection_status(self.gateway_name, False)
            self.gateway.write_log(f"交易接口：{self.gateway_name}WEBSOCKET API收到错误信息：{msg}")
            return
        # 事件类型
        channel = packet["subject"]
        function = self.func_map.get(channel, None)
        if function:
            function(packet)
    # ----------------------------------------------------------------------------------------------------
    def on_bbo(self, packet: dict):
        """
        收到逐笔一档价格推送
        """
        data = packet["data"]
        symbol = data["symbol"]
        tick = self.ticks[symbol]
        tick.datetime = get_local_datetime(data["ts"])
        tick.bid_price_1,tick.bid_volume_1 = float(data["bestBidPrice"]),float(data["bestBidSize"])
        tick.ask_price_1,tick.ask_volume_1 = float(data["bestAskPrice"]),float(data["bestAskSize"])
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_public_trade(self, packet: dict):
        """
        收到逐笔成交事件回报
        """
        data = packet["data"]
        symbol = data["symbol"]
        tick = self.ticks[symbol]
        tick.last_price = float(data["price"])
        tick.datetime = get_local_datetime(data["ts"])
    # ----------------------------------------------------------------------------------------------------
    def on_depth(self, packet: dict):
        """
        收到orderbook事件回报
        """
        symbol = packet["topic"].split(":")[1]
        tick = self.ticks[symbol]
        data = packet["data"]
        tick.datetime = get_local_datetime(data["timestamp"])

        # 封装更新order book的逻辑到一个辅助函数
        def update_order_book(order_books, type_prefix):
            for index, (price, volume) in enumerate(order_books, start=1):
                setattr(tick, f"{type_prefix}_price_{index}", float(price))
                setattr(tick, f"{type_prefix}_volume_{index}", float(volume))

        # 更新买单和卖单的order book
        update_order_book(data["bids"], "bid")
        update_order_book(data["asks"], "ask")
        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_position(self, packet: dict):
        """
        收到仓位事件回报
        """
        data = packet["data"]
        if "currentQty" not in data:
            return
        # 确定仓位方向
        volume = float(data["currentQty"])
        direction = Direction.LONG if volume >= 0 else Direction.SHORT

        # 创建仓位数据对象
        position = PositionData(
            symbol=data["symbol"],
            exchange=Exchange.KUCOIN,
            direction=direction,
            volume=abs(volume),
            price=float(data["avgEntryPrice"]) if data["avgEntryPrice"] else 0,
            pnl=float(data["unrealisedPnl"]),
            gateway_name=self.gateway_name,
        )

        # 创建对立仓位数据对象，其volume、price和pnl默认为0
        opposite_position = PositionData(
            symbol=data["symbol"],
            exchange=Exchange.KUCOIN,
            gateway_name=self.gateway_name,
            direction=OPPOSITE_DIRECTION[direction],
            volume=0,  # 对立仓位的量始终为0
            price=0,  # 对立仓位的价格始终为0
            pnl=0,  # 对立仓位的盈亏始终为0
        )

        # 发送仓位数据
        self.gateway.on_position(position)
        self.gateway.on_position(opposite_position)
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, packet: dict):
        """
        收到委托事件回报
        """
        data = packet["data"]
        volume = float(data["size"])
        trade_volume = float(data["filledSize"])
        if data["status"] == "done":
            if volume == trade_volume:
                status = Status.ALLTRADED
            if float(data["canceledSize"]):
                status = Status.CANCELLED
        elif data["status"] == "open":
            status = Status.NOTTRADED
        elif data["status"] == "match":
            if volume == trade_volume:
                status = Status.ALLTRADED
            elif volume > trade_volume:
                status = Status.PARTTRADED

        if "clientOid" in data:
            orderid = data["clientOid"]
        else:
            orderid = data["orderId"]
        if data["status"] == "match":
            price = float(data["matchPrice"])
        else:
            price = float(data["price"])
        order: OrderData = OrderData(
            orderid=orderid,
            symbol=data["symbol"],
            exchange=Exchange.KUCOIN,
            price=price,
            volume=volume,
            direction=DIRECTION_KUCOIN2VT[data["side"]],
            traded=trade_volume,
            status=status,
            datetime=get_local_datetime(data["orderTime"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_order(order)
        # orderid_map删除非活动委托单
        orderid_map = self.gateway.rest_api.orderid_map
        if not order.is_active():
            if orderid in orderid_map:
                orderid_map.pop(orderid)
        else:
            orderid_map[orderid] = data["orderId"]
        if order.traded:
            self.trade_id += 1
            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=Exchange.KUCOIN,
                orderid=order.orderid,
                tradeid=self.trade_id,
                direction=DIRECTION_KUCOIN2VT[data["side"]],
                price=order.price,
                volume=trade_volume,
                datetime=get_local_datetime(data["orderTime"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
