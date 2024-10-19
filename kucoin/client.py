from .margin.margin import MarginData
from .market.market import MarketData
from .trade.trade import TradeData
from .user.user import UserData
from .ws_token.token import GetToken


class User(UserData):
    pass


class Trade(TradeData):
    pass


class Market(MarketData):
    pass


class Margin(MarginData):
    pass


class WsToken(GetToken):
    pass


