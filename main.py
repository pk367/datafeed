import datetime
import enum
import json
import logging
import random
import re
import string
import pandas as pd
from websocket import create_connection, WebSocketTimeoutException
import requests

logger = logging.getLogger(__name__)


class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"


class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 5

    def __init__(
        self,
        username: str = None,
        password: str = None,
        auth_token: str = 'eyJhbGciOiJSUzUxMiIsImtpZCI6IkdaeFUiLCJ0eXAiOiJKV1QifQ.eyJ1c2VyX2lkIjo1NDgwNTUyMSwiZXhwIjoxNzI3ODIxNzk0LCJpYXQiOjE3Mjc4MDczOTQsInBsYW4iOiJwcm9fcHJlbWl1bSIsImV4dF9ob3VycyI6MSwicGVybSI6IiIsInN0dWR5X3Blcm0iOiJQVUI7NmEyZWI4MDUxMjc5NDkxOTk0MDk2MWQ4ZjUxMTYwMzUsUFVCOzQ2NjMzNTU3ZDE5YjQwYjE5MjQzMmVmNmYyOTA4ZDE0LFBVQjtjMDYxOTIzODdlMmU0ZDJkYTdjZDEzNmVlMjgxOTVlNixQVUI7ZGJmZGMzZDNmNDA3NDIyZjkzYjE2NDAyYTFmOGMwMGQsUFVCO2YzM2IxNTAzZjIyMDQxYzViZTVmZTkxMzMxY2NjNDE4LFBVQjtjZTNhZDY5MzI5MjQ0NTQ5OTgwMjE3NmM2YzRiNzkxMSxQVUI7NmM5MWEyOTRiM2ViNGQ5MTg3MWZlNzFjMzYyY2VkNTEsUFVCOzdjNGIwOWIyYjRhNjQ5YTNhNjU0ZjY2ZWZmOTE1NTcwLHR2LWNoYXJ0cGF0dGVybnMsUFVCO2YwOTdhNzhmODM0MjRiNmY4MWMwODQ3ZTVjOTg4Y2M4LFBVQjtlYTA4M2Q5MDUzNjQ0NzZkOWRmN2FjODBhMjVkMjg1YixQVUI7NmZiZWE5YmRmMGNmNGQ2YWJiNmFjOWE4NmI4OGM5MjQsUFVCO2UxZjAwZjZlNWY2NjRkNGJiMzM2ZWE5NGVhYzlmY2FlLFBVQjszMGNiYzY5MWJiOTM0YTI1OTkyOGU3NzYxYzg5YjBmNCxQVUI7YTQ4MWI5YzMzMmEwNDZmNThiODhmYmVhYWY4YWFhOTgsUFVCOzNlODU4YWU2MTNiMjQwMGU4MzAyOTE5ZGQ0MWQzOTU4LFBVQjtjNzNlOWMzZDA0YTU0MWQwOTVkMGQ3MzVhYTdiZjU5MSxQVUI7NWEyMGMzZTY2MTdjNDJjNWFjMmRmYzZlZmYxMzlkZjIsUFVCOzAzNzA2NmMyY2VjZTRjYTk4MzI3ODA0MTIzZWFiMjcyLHR2LXByb3N0dWRpZXMsdHYtY2hhcnRfcGF0dGVybnMsUFVCOzJkZDYyMDBkNmJmNjQ1OTI5MGFiNzJiZDZiODQyMzkxLFBVQjtiM2Y3YzBiY2I0NWQ0MzJjYThkNDc4NDczMGE5YzIyMyx0di12b2x1bWVieXByaWNlLFBVQjszOTVkNjAyYzVlZTI0ODMwYTc1MjZjYWFmN2MyNjI3YyIsIm1heF9zdHVkaWVzIjoyNSwibWF4X2Z1bmRhbWVudGFscyI6MTAsIm1heF9jaGFydHMiOjgsIm1heF9hY3RpdmVfYWxlcnRzIjo0MDAsIm1heF9zdHVkeV9vbl9zdHVkeSI6MjQsImZpZWxkc19wZXJtaXNzaW9ucyI6WyJyZWZib25kcyJdLCJtYXhfb3ZlcmFsbF9hbGVydHMiOjIwMDAsIm1heF9hY3RpdmVfcHJpbWl0aXZlX2FsZXJ0cyI6NDAwLCJtYXhfYWN0aXZlX2NvbXBsZXhfYWxlcnRzIjo0MDAsIm1heF9jb25uZWN0aW9ucyI6NTB9.cAWGPm30ohtUEBNo1ryPxzxSgkzEHR_zjk8jS3cDI-ERQl2iv-Cs6x-ozIA4HQm9pwF607wk-XMl7AGWdBpRxCaTstq8COYg1pzCbPYYQDW3MIXLG_sL96PZDATfb-EXN3O4ZIPSffAv38QKeBHvnRuU7KbqS_GKjRp3pSXrtAc',
    ) -> None:
        self.ws_debug = False

        if auth_token:
            self.token = auth_token
        else:
            self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            logger.warning("No valid auth token provided, using unauthorized token. Access may be limited.")

        self.ws = None
        self.session = self.__generate_session()
        self.chart_session = self.__generate_chart_session()

    def __auth(self, username, password):
        if username is None or password is None:
            logger.warning("Username or password is None.")
            return None

        data = {"username": username, "password": password, "remember": "on"}

        try:
            response = requests.post(url=self.__sign_in_url, data=data, headers=self.__signin_headers)
            response.raise_for_status()
            response_data = response.json()

            if 'user' in response_data and 'auth_token' in response_data['user']:
                return response_data['user']['auth_token']
            else:
                logger.error("Authorization failed. Response: %s", response_data)
                return None

        except requests.exceptions.RequestException as e:
            logger.error("Sign-in request failed: %s", e)
            return None

    def __create_connection(self):
        try:
            self.ws = create_connection(
                "wss://data.tradingview.com/socket.io/websocket",
                headers=self.__ws_headers,
                timeout=self.__ws_timeout
            )
        except WebSocketTimeoutException:
            logger.error("WebSocket connection timed out.")
            raise
        except Exception as e:
            logger.error("Failed to create WebSocket connection: %s", e)
            raise

    @staticmethod
    def __filter_raw_message(text):
        try:
            found = re.search(r'"m":"(.+?)",', text).group(1)
            found2 = re.search(r'"p":(.+?"}"])}', text).group(1)
            return found, found2
        except AttributeError as e:
            logger.error("Failed to parse WebSocket message: %s", e)
            raise

    @staticmethod
    def __generate_session():
        return "qs_" + ''.join(random.choice(string.ascii_lowercase) for _ in range(12))

    @staticmethod
    def __generate_chart_session():
        return "cs_" + ''.join(random.choice(string.ascii_lowercase) for _ in range(12))

    @staticmethod
    def __prepend_header(st):
        return f"~m~{len(st)}~m~{st}"

    @staticmethod
    def __construct_message(func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func, param_list):
        return self.__prepend_header(self.__construct_message(func, param_list))

    def __send_message(self, func, args):
        if self.ws is None:
            logger.error("WebSocket connection is not established.")
            raise ConnectionError("WebSocket is not connected.")

        try:
            self.ws.send(self.__create_message(func, args))
        except Exception as e:
            logger.error("Failed to send message: %s", e)
            raise

    def __create_df(self, raw_data, symbol):
        try:
            out = re.search(r'"s":\[(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = []
            volume_data = True

            for xi in x:
                xi = re.split(r"[\[:,\]]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]))
                row = [ts]

                for i in range(5, 10):
                    try:
                        row.append(float(xi[i]))
                    except (ValueError, IndexError):
                        if i == 9:
                            volume_data = False
                            row.append(0.0)
                            logger.debug("Volume data missing for %s", symbol)
                        else:
                            raise

                data.append(row)

            df = pd.DataFrame(
                data, columns=["datetime", "open", "high", "low", "close", "volume"]
            ).set_index("datetime")
            df.insert(0, "symbol", symbol)
            return df

        except AttributeError:
            logger.error("No data found for symbol %s. Check exchange and symbol.", symbol)
            raise ValueError(f"No data available for {symbol}")
        except Exception as e:
            logger.error("Error creating DataFrame for %s: %s", symbol, e)
            raise

    @staticmethod
    def __format_symbol(symbol, exchange, contract=None):
        if ":" in symbol:
            return symbol
        if contract is None:
            return f"{exchange}:{symbol}"
        if isinstance(contract, int):
            return f"{exchange}:{symbol}{contract}!"
        raise ValueError(f"Invalid contract type {contract}. Must be int or None.")

    def get_hist(
        self,
        symbol: str,
        exchange: str = "NSE",
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
    ) -> pd.DataFrame:
        try:
            formatted_symbol = self.__format_symbol(symbol, exchange, fut_contract)
        except ValueError as e:
            logger.error(
                "Symbol formatting error for %s/%s: %s",
                symbol, exchange, e
            )
            raise

        interval = interval.value
        raw_data = ""

        try:
            self.__create_connection()
            self.__send_message("set_auth_token", [self.token])
            self.__send_message("chart_create_session", [self.chart_session, ""])
            self.__send_message("quote_create_session", [self.session])
            self.__send_message("quote_set_fields", [
                self.session,
                "ch", "chp", "current_session", "description", "local_description",
                "language", "exchange", "fractional", "is_tradable", "lp", "lp_time",
                "minmov", "minmove2", "original_name", "pricescale", "pro_name",
                "short_name", "type", "update_mode", "volume", "currency_code", "rchp", "rtc"
            ])
            self.__send_message("quote_add_symbols", [self.session, formatted_symbol, {"flags": ["force_permission"]}])
            self.__send_message("quote_fast_symbols", [self.session, formatted_symbol])
            self.__send_message("resolve_symbol", [
                self.chart_session,
                "symbol_1",
                json.dumps({
                    "symbol": formatted_symbol,
                    "adjustment": "splits",
                    "session": "extended" if extended_session else "regular"
                })
            ])
            self.__send_message("create_series", [
                self.chart_session, "s1", "s1", "symbol_1", interval, n_bars
            ])
            self.__send_message("switch_timezone", [self.chart_session, "exchange"])

            logger.info("Fetching data for %s on %s", formatted_symbol, exchange)

            while True:
                try:
                    result = self.ws.recv()
                    raw_data += result + "\n"
                    if "series_completed" in result:
                        break
                except Exception as e:
                    logger.error(
                        "Data reception error for %s/%s: %s",
                        formatted_symbol, exchange, e
                    )
                    break

            return self.__create_df(raw_data, formatted_symbol)

        except Exception as e:
            logger.error(
                "Failed to retrieve data for %s/%s: %s",
                formatted_symbol, exchange, e
            )
            raise
        finally:
            if self.ws:
                self.ws.close()
                self.ws = None

    def search_symbol(self, text: str, exchange: str = ''):
        url = self.__search_url.format(text, exchange)
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return json.loads(resp.text.replace('</em>', '').replace('<em>', ''))
        except Exception as e:
            logger.error(
                "Search failed for '%s' on '%s': %s",
                text, exchange, e
            )
            return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    tv = TvDatafeed()
    #print(tv.get_hist("CRUDEOIL", "MCX", fut_contract=1))
    #print(tv.get_hist("NIFTY", "NSE", Interval.in_1_hour, 5))
    #print(tv.search_symbol("AAPL", "NASDAQ"))
