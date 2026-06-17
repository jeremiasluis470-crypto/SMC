# =============================================================================
#  SEVEN LEVELS BOT — MA7 + MACD
#  MODO SNIPER SUICIDA: dispara rápido, 10-15 trades/min, reinveste tudo
#  Modos: Conservador | Moderado | Suicida | 🎯 SNIPER SUICIDA
#  API: Nova Deriv API (PAT → REST → OTP → WebSocket)
# =============================================================================

import streamlit as st
import asyncio
import threading
import json
import time
import statistics
import os
import aiohttp
from datetime import datetime
from collections import deque
from dataclasses import dataclass
from typing import Optional
import pandas as pd
import websockets

# ─────────────────────────────────────────────────────────────────────────────
#  ATIVOS (Opções Binárias Deriv — índices sintéticos)
# ─────────────────────────────────────────────────────────────────────────────

VOLATILITY_SYMBOLS = {
    "R_10":     "Volatility 10 Index",
    "R_25":     "Volatility 25 Index",
    "R_50":     "Volatility 50 Index",
    "R_75":     "Volatility 75 Index",
    "R_100":    "Volatility 100 Index",
    "1HZ10V":  "Volatility 10 (1s) Index",
    "1HZ25V":  "Volatility 25 (1s) Index",
    "1HZ50V":  "Volatility 50 (1s) Index",
    "1HZ75V":  "Volatility 75 (1s) Index",
    "1HZ100V": "Volatility 100 (1s) Index",
    "CRASH300":  "Crash 300 Index",
    "CRASH500":  "Crash 500 Index",
    "CRASH1000": "Crash 1000 Index",
    "BOOM300":   "Boom 300 Index",
    "BOOM500":   "Boom 500 Index",
    "BOOM1000":  "Boom 1000 Index",
    "stpRNG":    "Step Index",
    "JD10":  "Jump 10 Index",
    "JD25":  "Jump 25 Index",
    "JD50":  "Jump 50 Index",
    "JD75":  "Jump 75 Index",
    "JD100": "Jump 100 Index",
}

SYMBOL_GROUPS = {
    "📊 Volatility (Recomendado)": ["R_10","R_25","R_50","R_75","R_100"],
    "⚡ Volatility 1s (Sniper)":   ["1HZ10V","1HZ25V","1HZ50V","1HZ75V","1HZ100V"],
    "💥 Crash & Boom":             ["CRASH300","CRASH500","CRASH1000","BOOM300","BOOM500","BOOM1000"],
    "🪜 Step Index":               ["stpRNG"],
    "🦘 Jump Indices":             ["JD10","JD25","JD50","JD75","JD100"],
}

# ─────────────────────────────────────────────────────────────────────────────
#  MODOS DO BOT
# ─────────────────────────────────────────────────────────────────────────────

MODOS = {
    "conservador": {
        "label":       "🟢 Conservador",
        "desc":        "Aposta fixa. Para após N perdas seguidas.",
        "css":         "mode-c",
        "min_conf":    0.75,
        "cooldown":    30,    # segundos entre trades
        "max_per_min": 2,
    },
    "moderado": {
        "label":       "🟡 Moderado",
        "desc":        "Cresce 20% por win. Reset em loss.",
        "css":         "mode-m",
        "min_conf":    0.65,
        "cooldown":    15,
        "max_per_min": 4,
    },
    "suicida": {
        "label":       "🔴 Suicida",
        "desc":        "Reinveste TUDO. Uma perda reinicia. Como no vídeo.",
        "css":         "mode-s",
        "min_conf":    0.55,
        "cooldown":    5,
        "max_per_min": 12,
    },
    "sniper": {
        "label":       "🎯 SNIPER SUICIDA",
        "desc":        "🔥 MODO MÁXIMO: dispara em cada sinal válido. 10-15 trades/min. Reinveste tudo. Sem cooldown. Alto risco extremo.",
        "css":         "mode-x",
        "min_conf":    0.50,  # aceita sinais mais fracos para disparar mais
        "cooldown":    0,     # sem cooldown — dispara logo
        "max_per_min": 20,    # sem limite prático
    },
}

# ─────────────────────────────────────────────────────────────────────────────
#  DURAÇÃO DOS CONTRATOS
# ─────────────────────────────────────────────────────────────────────────────

CONTRACT_DURATIONS = {
    "1 tique":   (1,  "t"),
    "2 tiques":  (2,  "t"),
    "5 tiques":  (5,  "t"),
    "10 tiques": (10, "t"),
    "15s":  (15, "s"),
    "30s":  (30, "s"),
    "60s":  (60, "s"),
    "2m":   (2,  "m"),
    "5m":   (5,  "m"),
    "15m":  (15, "m"),
    "30m":  (30, "m"),
    "1h":   (1,  "h"),
}

CHART_GRANULARITY = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "30m": 1800,
    "1h":  3600,
}

# ─────────────────────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candle:
    open:  float
    high:  float
    low:   float
    close: float
    epoch: int = 0

    @property
    def body(self):      return abs(self.close - self.open)
    @property
    def upper_wick(self):return self.high - max(self.open, self.close)
    @property
    def lower_wick(self):return min(self.open, self.close) - self.low
    @property
    def is_bullish(self):return self.close > self.open
    @property
    def is_bearish(self):return self.close < self.open
    @property
    def range(self):     return self.high - self.low


@dataclass
class Signal:
    direction:  str
    confidence: float
    reason:     str
    ma_score:   float = 0.0
    macd_score: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  INDICADORES MA7 + MACD
# ─────────────────────────────────────────────────────────────────────────────

def _ema(prices, period):
    if len(prices) < period: return prices[:]
    k = 2 / (period + 1)
    out = [prices[0]]
    for p in prices[1:]:
        out.append(p * k + out[-1] * (1 - k))
    return out

def _sma(prices, period):
    out = []
    for i in range(len(prices)):
        if i < period - 1: out.append(prices[i])
        else: out.append(statistics.mean(prices[i-period+1:i+1]))
    return out


class MA7Indicator:
    PERIOD = 7

    def analyze(self, closes):
        if len(closes) < self.PERIOD + 2:
            return {"trend":"SIDEWAYS","touch":False,"near":False,
                    "distance":1.0,"score":0.0,"ma":None}
        ma     = _sma(closes, self.PERIOD)
        ma_now = ma[-1]
        price  = closes[-1]
        dist   = abs(price - ma_now) / (ma_now + 1e-9)
        trend  = "UP" if price > ma_now else ("DOWN" if price < ma_now else "SIDEWAYS")
        return {
            "trend":    trend,
            "touch":    dist < 0.0015,
            "near":     dist < 0.0035,
            "distance": dist,
            "score":    round(max(0.0, 1.0 - dist / 0.004), 2),
            "ma":       ma_now,
        }


class MACDIndicator:
    FAST=12; SLOW=26; SIGNAL=9

    def analyze(self, closes):
        if len(closes) < self.SLOW + self.SIGNAL + 2:
            return {"direction":"NEUTRAL","score":0.0,"growing":False}
        ema_f   = _ema(closes, self.FAST)
        ema_s   = _ema(closes, self.SLOW)
        macd    = [f-s for f,s in zip(ema_f, ema_s)]
        sig     = _ema(macd, self.SIGNAL)
        hist    = [m-s for m,s in zip(macd, sig)]
        mn, sn  = macd[-1], sig[-1]
        hn, hp  = hist[-1], (hist[-2] if len(hist)>1 else 0)

        if mn > sn and hn > 0:
            d = "BULL"; st = min(1.0, abs(hn)/(abs(mn)+1e-9)*5); g = hn > hp
        elif mn < sn and hn < 0:
            d = "BEAR"; st = min(1.0, abs(hn)/(abs(mn)+1e-9)*5); g = hn < hp
        else:
            d = "NEUTRAL"; st = 0.0; g = False

        return {"direction":d, "score":min(1.0,round(st*(1.2 if g else 0.8),2)), "growing":g}


# ─────────────────────────────────────────────────────────────────────────────
#  ENGINE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class SevenLevelsEngine:
    def __init__(self, min_conf=0.55):
        self.min_conf = min_conf
        self.ma7  = MA7Indicator()
        self.macd = MACDIndicator()

    def _consec(self, candles, bullish):
        count = 0
        for c in reversed(candles[:-1]):
            if (bullish and c.is_bullish) or (not bullish and c.is_bearish): count += 1
            else: break
        return count

    def evaluate(self, candles) -> Signal:
        if len(candles) < 35:
            return Signal("WAIT", 0.0, "aguardando candles...")

        closes   = [c.close for c in candles]
        ma_r     = self.ma7.analyze(closes)
        macd_r   = self.macd.analyze(closes)
        trend    = ma_r["trend"]
        macd_dir = macd_r["direction"]
        macd_g   = macd_r["growing"]

        if trend == "SIDEWAYS":
            return Signal("WAIT", 0.0, "MA7: lateral")

        base_dir    = "CALL" if trend == "UP" else "PUT"
        macd_expect = "BULL" if base_dir == "CALL" else "BEAR"

        if macd_dir not in ("NEUTRAL", macd_expect):
            return Signal("WAIT", 0.0, f"MACD contra MA7 ({macd_dir} vs {macd_expect})")

        consec = self._consec(candles, base_dir == "CALL")

        touch_b = 0.25 if ma_r["touch"] else (0.10 if ma_r["near"] else 0.0)
        macd_b  = macd_r["score"] * 0.35
        cons_b  = min(0.20, consec * 0.05)
        grow_b  = 0.10 if macd_g else 0.0
        conf    = round(min(1.0, 0.30 + touch_b + macd_b + cons_b + grow_b), 2)

        if conf < self.min_conf:
            return Signal("WAIT", conf,
                f"conf {conf:.2f}<{self.min_conf} | dist:{ma_r['distance']*100:.2f}%")

        ts = ("🎯TOQUE" if ma_r["touch"] else
              f"perto({ma_r['distance']*100:.2f}%)" if ma_r["near"]
              else f"fluxo({ma_r['distance']*100:.2f}%)")
        reason = (f"{base_dir}|{ts}|MACD:{macd_dir}({macd_r['score']:.2f})"
                  f"{'📈' if macd_g else ''}|velas:{consec}|conf:{conf:.2f}")

        return Signal(base_dir, conf, reason, ma_r["score"], macd_r["score"])


# ─────────────────────────────────────────────────────────────────────────────
#  COMPOUNDING MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class CompoundingManager:
    def __init__(self, mode, base_stake):
        self.mode       = mode
        self.base_stake = base_stake
        self.current    = base_stake
        self.level      = 1
        self.peak       = base_stake
        self.session_pnl= 0.0  # P&L acumulado do modo suicida nesta corrida

    def next_stake(self): return round(max(0.35, self.current), 2)

    def on_win(self, profit):
        self.session_pnl += profit
        if self.mode in ("suicida", "sniper"):
            self.current = round(self.current + profit, 2)
            self.peak    = max(self.peak, self.current)
        elif self.mode == "moderado":
            self.current = round(min(self.current * 1.20, self.current * 5), 2)
        else:
            self.current = self.base_stake
        self.level += 1

    def on_loss(self, loss):
        self.session_pnl -= abs(loss)
        self.current = self.base_stake
        self.level   = 1

    def reset(self):
        self.current     = self.base_stake
        self.level       = 1
        self.session_pnl = 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  SESSION MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class SessionManager:
    def __init__(self):
        self._lock          = threading.Lock()
        self._trades        = []
        self._logs          = deque(maxlen=500)
        self._signals       = deque(maxlen=100)
        self._pnl           = 0.0
        self._wins          = 0
        self._losses        = 0
        self._consec_losses = 0
        self._max_consec    = 0
        self._running       = False
        self._stop_reason   = ""
        self._trades_min    = deque(maxlen=200)  # timestamps para trades/min

    def set_running(self, v, reason=""):
        with self._lock:
            self._running = v; self._stop_reason = reason

    def is_running(self):
        with self._lock: return self._running

    def stop_reason(self):
        with self._lock: return self._stop_reason

    def trades_per_min(self):
        with self._lock:
            now = time.time()
            recent = [t for t in self._trades_min if now - t < 60]
            return len(recent)

    def add_trade(self, symbol, direction, stake, profit, level=1, reason=""):
        with self._lock:
            entry = {
                "time":      datetime.now().strftime("%H:%M:%S"),
                "symbol":    VOLATILITY_SYMBOLS.get(symbol, symbol),
                "direction": direction,
                "level":     level,
                "stake":     round(stake, 2),
                "profit":    round(profit, 2),
                "signal":    reason[:50],
            }
            self._trades.append(entry)
            self._trades_min.append(time.time())
            self._pnl += profit
            if profit > 0:
                self._wins += 1; self._consec_losses = 0
            else:
                self._losses += 1; self._consec_losses += 1
                self._max_consec = max(self._max_consec, self._consec_losses)
            r = f"✅+${profit:.2f}" if profit>0 else f"❌-${abs(profit):.2f}"
            self._logs.append(
                f"[{entry['time']}] Nv{level} {direction} ${stake:.2f} {r}")

    def add_signal(self, direction, reason):
        with self._lock:
            self._signals.append({
                "time": datetime.now().strftime("%H:%M:%S"),
                "dir":  direction, "reason": reason})

    def log(self, msg):
        with self._lock:
            self._logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def get_trades(self):
        with self._lock: return list(self._trades)
    def get_signals(self):
        with self._lock: return list(self._signals)
    def get_logs(self):
        with self._lock: return list(self._logs)
    def consec_losses(self):
        with self._lock: return self._consec_losses
    def pnl(self):
        with self._lock: return self._pnl

    def stats(self):
        with self._lock:
            total   = self._wins + self._losses
            winrate = (self._wins/total*100) if total>0 else 0.0
            return {
                "pnl":           round(self._pnl,2),
                "trades":        total,
                "wins":          self._wins,
                "losses":        self._losses,
                "winrate":       winrate,
                "consec_losses": self._consec_losses,
                "max_consec":    self._max_consec,
            }

    def reset(self):
        with self._lock:
            self._trades=[]; self._pnl=0.0
            self._wins=0; self._losses=0
            self._consec_losses=0; self._max_consec=0
            self._signals.clear(); self._logs.clear()
            self._trades_min.clear()
            self._running=False; self._stop_reason=""


# ─────────────────────────────────────────────────────────────────────────────
#  DERIV CLIENT
# ─────────────────────────────────────────────────────────────────────────────

DERIV_REST_BASE = "https://api.derivws.com"

class DerivClient:
    def __init__(self, pat_token, app_id, account_type="demo"):
        self.pat=pat_token; self.app_id=app_id; self.account_type=account_type
        self._ws=None; self._req_id=1; self._pending={}
        self._candles_q=asyncio.Queue(maxsize=5000)
        self._ticks_q=asyncio.Queue(maxsize=5000)
        self._listener_task=None; self._account_id=None

    def _h(self):
        return {"Authorization":f"Bearer {self.pat}",
                "Deriv-App-ID":self.app_id,"Content-Type":"application/json"}

    async def _get_account_id(self):
        url=f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.get(url,headers=self._h()) as r:
                body=await r.json()
                if r.status!=200: raise PermissionError(f"Erro contas: {body}")
                for acc in body.get("data",[]):
                    if acc.get("account_type")==self.account_type and acc.get("status")=="active":
                        return acc["account_id"]
                if self.account_type=="demo": return await self._create_demo()
                raise RuntimeError(f"Conta '{self.account_type}' não encontrada.")

    async def _create_demo(self):
        url=f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.post(url,headers=self._h(),
                              json={"currency":"USD","group":"row","account_type":"demo"}) as r:
                body=await r.json()
                if r.status not in (200,201): raise RuntimeError(f"Erro demo: {body}")
                return body["data"]["account_id"]

    async def _get_ws_url(self, account_id):
        url=f"{DERIV_REST_BASE}/trading/v1/options/accounts/{account_id}/otp"
        async with aiohttp.ClientSession() as s:
            async with s.post(url,headers=self._h()) as r:
                body=await r.json()
                if r.status!=200: raise PermissionError(f"Erro OTP: {body}")
                ws_url=body.get("data",{}).get("url")
                if not ws_url: raise RuntimeError(f"URL WS não encontrado: {body}")
                return ws_url

    async def connect(self, retries=3):
        last=None
        for i in range(1,retries+1):
            try:
                self._account_id=await self._get_account_id()
                ws_url=await self._get_ws_url(self._account_id)
                self._ws=await websockets.connect(
                    ws_url,ping_interval=30,ping_timeout=10,close_timeout=5)
                self._listener_task=asyncio.create_task(self._listener())
                return
            except PermissionError: raise
            except Exception as e:
                last=e
                if i<retries: await asyncio.sleep(3*i)
        raise ConnectionError(f"Falha após {retries} tentativas: {last}")

    async def disconnect(self):
        if self._listener_task: self._listener_task.cancel()
        if self._ws:
            try: await self._ws.close()
            except: pass

    async def _send(self, payload, timeout=20.0):
        req_id=self._req_id; self._req_id+=1
        payload["req_id"]=req_id
        fut=asyncio.get_event_loop().create_future()
        self._pending[req_id]=fut
        await self._ws.send(json.dumps(payload))
        try: return await asyncio.wait_for(fut,timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id,None)
            raise TimeoutError(f"Timeout req {req_id}")

    async def _listener(self):
        try:
            async for raw in self._ws:
                msg=json.loads(raw)
                req_id=msg.get("req_id")
                if req_id and req_id in self._pending:
                    fut=self._pending.pop(req_id)
                    if not fut.done(): fut.set_result(msg)
                elif msg.get("msg_type")=="ohlc":
                    await self._candles_q.put(msg)
                elif msg.get("msg_type")=="tick":
                    await self._ticks_q.put(msg)
        except (asyncio.CancelledError,websockets.ConnectionClosed): pass

    async def subscribe_candles(self, symbol, granularity=60):
        resp=await self._send({"ticks_history":symbol,"style":"candles",
                               "granularity":granularity,"count":100,
                               "end":"latest","subscribe":1})
        if resp.get("error"): raise RuntimeError(resp["error"]["message"])
        return resp.get("candles",[])

    async def subscribe_ticks(self, symbol):
        """Subscreve ticks em tempo real para o modo sniper"""
        resp=await self._send({"ticks":symbol,"subscribe":1})
        if resp.get("error"): raise RuntimeError(resp["error"]["message"])

    async def get_candle_update(self, timeout=180.0):
        return await asyncio.wait_for(self._candles_q.get(), timeout)

    async def get_tick(self, timeout=10.0):
        return await asyncio.wait_for(self._ticks_q.get(), timeout)

    async def buy_contract(self, symbol, direction, stake, duration, duration_unit="t"):
        proposal=await self._send({
            "proposal":1,"amount":stake,"basis":"stake",
            "contract_type":direction,"currency":"USD",
            "duration":duration,"duration_unit":duration_unit,
            "underlying_symbol":symbol})
        if proposal.get("error"): raise RuntimeError(proposal["error"]["message"])
        buy=await self._send({"buy":proposal["proposal"]["id"],"price":stake})
        if buy.get("error"): raise RuntimeError(buy["error"]["message"])
        return buy["buy"]

    async def get_contract_result(self, contract_id, max_wait=300.0):
        deadline=asyncio.get_event_loop().time()+max_wait
        while asyncio.get_event_loop().time()<deadline:
            resp=await self._send({"proposal_open_contract":1,"contract_id":contract_id})
            poc=resp.get("proposal_open_contract",{})
            if poc.get("is_sold") or poc.get("status") in ("sold","won","lost"):
                return {"profit":float(poc.get("profit",0)),"status":poc.get("status")}
            await asyncio.sleep(1)
        raise TimeoutError("Contrato não liquidou")

    async def get_balance(self):
        url=f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.get(url,headers=self._h()) as r:
                body=await r.json()
                for acc in body.get("data",[]):
                    if acc.get("account_id")==self._account_id:
                        return float(acc.get("balance",0))
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  BOT PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class SevenLevelsBot:
    def __init__(self, config, manager):
        self.cfg     = config
        self.manager = manager
        self._stop   = False

        self.symbol   = config["symbol"]
        self.mode     = config["mode"]
        self.modo_cfg = MODOS[self.mode]

        # Contrato
        self.contract_dur = config["contract_duration"]
        self.dur_val, self.dur_unit = CONTRACT_DURATIONS.get(self.contract_dur,(5,"t"))

        # Candles para análise
        self.chart_tf    = config["chart_tf"]
        self.granularity = CHART_GRANULARITY.get(self.chart_tf, 300)

        # Capital
        self.base_stake = float(config["stake"])
        self.goal_pnl   = float(config["goal"])
        self.stop_pnl   = float(config["stop_loss"])
        self.max_consec = int(config["max_consec"])

        # Engine com confiança mínima do modo
        self.engine   = SevenLevelsEngine(min_conf=self.modo_cfg["min_conf"])
        self.compound = CompoundingManager(mode=self.mode, base_stake=self.base_stake)
        self.client   = DerivClient(config["api_token"],config["app_id"],
                                    config.get("account_type","demo"))
        self.candles  = []
        self._in_trade = False  # evitar dois trades ao mesmo tempo

    def stop(self): self._stop = True

    async def _check_limits(self):
        """Retorna (parar, razão) se atingiu algum limite."""
        pnl   = self.manager.pnl()
        stats = self.manager.stats()

        if pnl >= self.goal_pnl:
            return True, f"🎯 META ATINGIDA! Ganho: +${pnl:.2f} / Meta: +${self.goal_pnl:.2f}"
        if pnl <= -self.stop_pnl:
            return True, f"🛑 STOP LOSS! Perda: -${abs(pnl):.2f} / Limite: -${self.stop_pnl:.2f}"
        if stats["consec_losses"] >= self.max_consec and self.mode not in ("suicida","sniper"):
            return True, f"🛑 {stats['consec_losses']} perdas consecutivas!"
        return False, ""

    async def _execute_trade(self, signal):
        """Executa um trade e regista o resultado."""
        if self._in_trade:
            return  # já há um trade a decorrer
        self._in_trade = True
        stake = self.compound.next_stake()
        level = self.compound.level
        try:
            self.manager.log(
                f"🎯 {signal.direction} | Nv{level} | ${stake:.2f} | "
                f"conf={signal.confidence:.2f} | {signal.reason[:60]}")

            buy_info    = await self.client.buy_contract(
                self.symbol, signal.direction,
                stake, self.dur_val, self.dur_unit)
            contract_id = buy_info.get("contract_id")

            result  = await self.client.get_contract_result(contract_id)
            profit  = result["profit"]

            self.manager.add_trade(
                self.symbol, signal.direction,
                stake, profit, level, signal.reason[:50])

            if profit > 0:
                self.compound.on_win(profit)
                self.manager.log(
                    f"✅ WIN +${profit:.2f} | "
                    f"Nv{self.compound.level} | "
                    f"Próx: ${self.compound.current:.2f} | "
                    f"P&L: ${self.manager.pnl():+.2f}")
            else:
                self.compound.on_loss(abs(profit))
                self.manager.log(
                    f"❌ LOSS -${abs(profit):.2f} | "
                    f"P&L: ${self.manager.pnl():+.2f} | "
                    f"Reinicia: ${self.base_stake:.2f}")

        except Exception as e:
            self.manager.log(f"❌ Erro trade: {e}")
        finally:
            self._in_trade = False

    async def run(self):
        label     = VOLATILITY_SYMBOLS.get(self.symbol, self.symbol)
        cooldown  = self.modo_cfg["cooldown"]
        is_sniper = self.mode == "sniper"

        self.manager.set_running(True)
        self.manager.log(f"🚀 {'🎯 SNIPER SUICIDA' if is_sniper else 'Seven Levels Bot'} INICIADO")
        self.manager.log(f"📌 {label} | {self.mode.upper()} | Contrato:{self.contract_dur} | Candles:{self.chart_tf}")
        self.manager.log(f"💰 Stake:${self.base_stake} | Meta:+${self.goal_pnl} | Stop:-${self.stop_pnl}")
        if is_sniper:
            self.manager.log("🔥 SNIPER MODE: sem cooldown — dispara em cada sinal válido!")

        last_trade_time = 0.0

        try:
            await self.client.connect()
            balance = await self.client.get_balance()
            self.manager.log(f"✅ Conectado | {self.client._account_id} | Saldo:${balance:.2f}")

            # Carregar candles históricos
            raw = await self.client.subscribe_candles(self.symbol, self.granularity)
            for r in raw:
                self.candles.append(Candle(
                    float(r["open"]),float(r["high"]),
                    float(r["low"]), float(r["close"]),
                    int(r.get("epoch",0))))
            self.candles = self.candles[-150:]
            self.manager.log(f"📊 {len(self.candles)} candles | aguardando sinais MA7+MACD...")

            # Sniper também subscreve ticks para reagir mais rápido
            if is_sniper:
                await self.client.subscribe_ticks(self.symbol)
                self.manager.log("⚡ Ticks em tempo real activados — modo sniper pronto!")

            while not self._stop:
                # ── Verificar limites ──────────────────────────────────────────
                parar, razao = await self._check_limits()
                if parar:
                    self.manager.log(razao)
                    self.manager.set_running(False, razao)
                    break

                # ── Aguardar actualização de preço ─────────────────────────────
                try:
                    if is_sniper:
                        # Sniper: usa ticks (mais rápido) + actualiza candles
                        try:
                            tick_msg = await self.client.get_tick(timeout=5.0)
                            tick     = tick_msg.get("tick",{})
                            if tick:
                                price = float(tick.get("quote",0))
                                epoch = int(tick.get("epoch",0))
                                # Actualiza o último candle com o tick actual
                                if self.candles:
                                    last = self.candles[-1]
                                    updated = Candle(
                                        last.open,
                                        max(last.high, price),
                                        min(last.low,  price),
                                        price, epoch)
                                    self.candles[-1] = updated
                        except asyncio.TimeoutError:
                            pass

                        # Também processa candles se chegarem
                        try:
                            while not self.client._candles_q.empty():
                                msg  = self.client._candles_q.get_nowait()
                                ohlc = msg.get("ohlc",{})
                                if ohlc:
                                    c = Candle(
                                        float(ohlc["open"]),float(ohlc["high"]),
                                        float(ohlc["low"]), float(ohlc["close"]),
                                        int(ohlc.get("epoch",0)))
                                    if not self.candles or c.epoch != self.candles[-1].epoch:
                                        self.candles.append(c)
                                        if len(self.candles)>150:
                                            self.candles=self.candles[-150:]
                        except: pass

                    else:
                        # Modos normais: espera candle completo
                        msg  = await self.client.get_candle_update(timeout=180)
                        ohlc = msg.get("ohlc",{})
                        if ohlc:
                            c = Candle(
                                float(ohlc["open"]),float(ohlc["high"]),
                                float(ohlc["low"]), float(ohlc["close"]),
                                int(ohlc.get("epoch",0)))
                            if not self.candles or c.epoch!=self.candles[-1].epoch:
                                self.candles.append(c)
                                if len(self.candles)>150:
                                    self.candles=self.candles[-150:]

                except asyncio.TimeoutError:
                    self.manager.log("⏳ Aguardando dados...")
                    continue

                if len(self.candles) < 35:
                    continue

                # ── Avaliar sinal ──────────────────────────────────────────────
                signal = self.engine.evaluate(self.candles)
                self.manager.add_signal(signal.direction, signal.reason)

                if signal.direction == "WAIT":
                    continue

                # ── Cooldown (sniper = 0s, outros têm cooldown) ────────────────
                now = time.time()
                if cooldown > 0 and (now - last_trade_time) < cooldown:
                    remaining = int(cooldown - (now - last_trade_time))
                    self.manager.add_signal("WAIT", f"cooldown {remaining}s")
                    continue

                # ── Não abrir se já há trade a decorrer ───────────────────────
                if self._in_trade:
                    if is_sniper:
                        self.manager.add_signal("WAIT", "trade em curso — aguarda")
                    continue

                last_trade_time = now

                # ── Executar (sniper: async sem bloquear o loop) ───────────────
                if is_sniper:
                    asyncio.create_task(self._execute_trade(signal))
                else:
                    await self._execute_trade(signal)

                # Pequena pausa para não sobrecarregar a API
                await asyncio.sleep(0.1 if is_sniper else 1)

        except Exception as e:
            self.manager.log(f"💥 Erro crítico: {e}")
            self.manager.set_running(False, f"Erro: {e}")
        finally:
            await self.client.disconnect()
            pnl = self.manager.pnl()
            self.manager.log(f"🔌 Bot encerrado | P&L sessão: ${pnl:+.2f}")
            if self.manager.is_running():
                self.manager.set_running(False, "Encerrado")


# ─────────────────────────────────────────────────────────────────────────────
#  STREAMLIT DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Seven Levels Bot",page_icon="🎯",
                   layout="wide",initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;700&display=swap');
html,body,[class*="css"]{font-family:'Space Grotesk',sans-serif;}
.stApp{background:#0a0e1a;color:#e0e6f5;}
.metric-card{background:#111827;border:1px solid #1e3a5f;border-radius:12px;padding:16px;text-align:center;}
.metric-label{font-size:.72rem;color:#7c9cbf;margin-bottom:4px;}
.profit{color:#00d4aa;font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;}
.loss  {color:#ff4d6d;font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;}
.neutral{color:#7c9cbf;font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;}
.warn  {color:#f59e0b;font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;}
.sniper{color:#ff6600;font-family:'JetBrains Mono',monospace;font-size:1.5rem;font-weight:700;}
.signal-box {background:#111827;border-left:4px solid #00d4aa;border-radius:8px;padding:8px 12px;margin:3px 0;font-family:'JetBrains Mono',monospace;font-size:.78rem;}
.signal-sell{border-left-color:#ff4d6d;}
.signal-wait{border-left-color:#f59e0b;}
.level-bar{background:#111827;border:1px solid #1e3a5f;border-radius:8px;padding:8px 12px;margin:3px 0;}
.banner-running {background:linear-gradient(135deg,#003320,#001a10);border:1px solid #00d4aa;border-radius:10px;padding:14px 20px;margin:8px 0;}
.banner-sniper  {background:linear-gradient(135deg,#2a1000,#1a0800);border:2px solid #ff6600;border-radius:10px;padding:14px 20px;margin:8px 0;animation:pulse 1s infinite;}
.banner-stopped {background:linear-gradient(135deg,#200a0a,#100505);border:1px solid #ff4d6d;border-radius:10px;padding:14px 20px;margin:8px 0;}
.banner-goal    {background:linear-gradient(135deg,#1a3300,#0d1a00);border:2px solid #00ff88;border-radius:10px;padding:18px;margin:8px 0;text-align:center;font-size:1.2rem;}
.mode-c{background:#0d2818;border-left:3px solid #00d4aa;border-radius:6px;padding:8px 12px;font-size:.82rem;color:#a0d8c0;margin:4px 0;}
.mode-m{background:#1a1a0d;border-left:3px solid #f59e0b;border-radius:6px;padding:8px 12px;font-size:.82rem;color:#d8c8a0;margin:4px 0;}
.mode-s{background:#200a0a;border-left:3px solid #ff4d6d;border-radius:6px;padding:8px 12px;font-size:.82rem;color:#d8a0a0;margin:4px 0;}
.mode-x{background:#1a0800;border-left:3px solid #ff6600;border-radius:6px;padding:8px 12px;font-size:.82rem;color:#ffa060;margin:4px 0;}
.dot-green{width:10px;height:10px;background:#00d4aa;border-radius:50%;display:inline-block;margin-right:6px;}
.dot-red  {width:10px;height:10px;background:#ff4d6d;border-radius:50%;display:inline-block;margin-right:6px;}
.dot-orange{width:10px;height:10px;background:#ff6600;border-radius:50%;display:inline-block;margin-right:6px;}
.stButton>button{background:linear-gradient(135deg,#00d4aa,#0099ff);color:#0a0e1a;font-weight:700;border:none;border-radius:8px;font-size:1rem;padding:10px;}
@keyframes pulse{0%{border-color:#ff6600;}50%{border-color:#ff9900;}100%{border-color:#ff6600;}}
</style>
""", unsafe_allow_html=True)

for k,v in [("bot",None),("running",False),("manager",SessionManager())]:
    if k not in st.session_state: st.session_state[k]=v

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🎯 Seven Levels Bot")
    st.markdown("*MA7 + MACD · 100% Automático*")
    st.markdown("---")

    api_key = st.text_input("🔑 PAT Token",type="password",
                             value=os.environ.get("DERIV_API_TOKEN",""))
    app_id  = st.text_input("🆔 App ID",
                             value=os.environ.get("DERIV_APP_ID",""))

    st.markdown("---")
    st.markdown("### 📊 Ativo")
    account_type = st.selectbox("Conta",["demo","real"])
    group  = st.selectbox("Grupo",list(SYMBOL_GROUPS.keys()))
    symbol = st.selectbox("Índice",SYMBOL_GROUPS[group],
                           format_func=lambda x:f"{x} — {VOLATILITY_SYMBOLS.get(x,x)}")

    st.markdown("---")
    st.markdown("### ⏱️ Duração do Contrato")
    contract_duration = st.selectbox(
        "Quanto dura a opção binária",
        list(CONTRACT_DURATIONS.keys()),
        index=3,
        help="Tempo real que o contrato fica aberto na Deriv")

    st.markdown("### 📈 Timeframe Candles (Análise)")
    chart_tf = st.selectbox(
        "Granularidade MA7+MACD",
        list(CHART_GRANULARITY.keys()),
        index=1,
        help="Candles usados para calcular MA7 e MACD — NÃO é a duração do contrato")

    st.markdown("---")
    st.markdown("### 🎮 Modo de Operação")
    mode = st.radio("",list(MODOS.keys()),
                    format_func=lambda x: MODOS[x]["label"])
    mc   = MODOS[mode]["css"]
    md   = MODOS[mode]["desc"]
    st.markdown(f'<div class="{mc}">{md}</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 💰 Capital")
    stake = st.number_input(
        "Aposta inicial ($)",
        min_value=0.35,
        max_value=10.0 if mode in ("suicida","sniper") else 500.0,
        value=1.0, step=0.35)

    goal = st.number_input(
        "Meta de ganho ($)",
        min_value=0.5,max_value=10000.0,
        value=1000.0 if mode in ("suicida","sniper") else 10.0,
        step=1.0,
        help="Bot para quando GANHAR este valor na sessão")

    stop_loss = st.number_input(
        "Stop Loss ($)",
        min_value=0.35,max_value=1000.0,
        value=1.0 if mode in ("suicida","sniper") else 5.0,
        step=0.35,
        help="Bot para quando PERDER este valor na sessão")

    max_consec = (999 if mode in ("suicida","sniper")
                  else int(st.number_input("Parar após N perdas seguidas",
                                           min_value=1,max_value=10,value=3)))

    st.markdown("---")
    c1,c2 = st.columns(2)
    start_btn = c1.button("▶ INICIAR",use_container_width=True)
    stop_btn  = c2.button("⏹ PARAR", use_container_width=True)

# ── HEADER ────────────────────────────────────────────────────────────────────
is_sniper  = mode == "sniper"
sym_label  = VOLATILITY_SYMBOLS.get(symbol, symbol)
manager    = st.session_state.manager
is_running = manager.is_running()
stop_rsn   = manager.stop_reason()

if is_sniper:
    st.markdown("# 🎯 SNIPER SUICIDA BOT")
else:
    st.markdown("# 🎯 Seven Levels Bot")
st.markdown(f"*MA7+MACD · **{sym_label}** · Contrato:{contract_duration} · Candles:{chart_tf} · {MODOS[mode]['label']}*")

if is_running and is_sniper:
    tpm = manager.trades_per_min()
    st.markdown(
        f'<div class="banner-sniper"><span class="dot-orange"></span>'
        f'<b>🔥 SNIPER A DISPARAR</b> — {sym_label} | '
        f'Trades/min: <b>{tpm}</b> | '
        f'P&L: <b>${manager.pnl():+.2f}</b></div>',
        unsafe_allow_html=True)
elif is_running:
    st.markdown(
        f'<div class="banner-running"><span class="dot-green"></span>'
        f'<b>BOT A OPERAR</b> — {sym_label} | {MODOS[mode]["label"]} | '
        f'Para quando ganhar ${goal:.2f} ou perder ${stop_loss:.2f}</div>',
        unsafe_allow_html=True)
elif "META" in stop_rsn:
    st.markdown(f'<div class="banner-goal">🏆 {stop_rsn}</div>',unsafe_allow_html=True)
elif stop_rsn:
    st.markdown(
        f'<div class="banner-stopped"><span class="dot-red"></span>'
        f'<b>PARADO:</b> {stop_rsn}</div>',unsafe_allow_html=True)
else:
    st.markdown(
        '<div class="banner-stopped"><span class="dot-red"></span>'
        '<b>OFFLINE</b> — Clica ▶ INICIAR</div>',unsafe_allow_html=True)

# ── MÉTRICAS ──────────────────────────────────────────────────────────────────
stats = manager.stats()
tpm   = manager.trades_per_min()

m1,m2,m3,m4,m5,m6,m7 = st.columns(7)
with m1:
    cls="profit" if stats["pnl"]>=0 else "loss"
    st.markdown(f'<div class="metric-card"><div class="metric-label">P&L SESSÃO</div>'
                f'<div class="{cls}">${stats["pnl"]:+.2f}</div></div>',unsafe_allow_html=True)
with m2:
    st.markdown(f'<div class="metric-card"><div class="metric-label">TRADES</div>'
                f'<div class="neutral">{stats["trades"]}</div></div>',unsafe_allow_html=True)
with m3:
    wc="profit" if stats["winrate"]>=60 else ("neutral" if stats["winrate"]>=50 else "loss")
    st.markdown(f'<div class="metric-card"><div class="metric-label">WIN RATE</div>'
                f'<div class="{wc}">{stats["winrate"]:.1f}%</div></div>',unsafe_allow_html=True)
with m4:
    st.markdown(f'<div class="metric-card"><div class="metric-label">WINS ✅</div>'
                f'<div class="profit">{stats["wins"]}</div></div>',unsafe_allow_html=True)
with m5:
    st.markdown(f'<div class="metric-card"><div class="metric-label">LOSSES ❌</div>'
                f'<div class="loss">{stats["losses"]}</div></div>',unsafe_allow_html=True)
with m6:
    cl=stats["consec_losses"]
    cl_c="loss" if cl>=3 else ("warn" if cl>=2 else "neutral")
    st.markdown(f'<div class="metric-card"><div class="metric-label">CONS.LOSS</div>'
                f'<div class="{cl_c}">{cl}</div></div>',unsafe_allow_html=True)
with m7:
    tpm_c="sniper" if tpm>=8 else ("profit" if tpm>=4 else "neutral")
    st.markdown(f'<div class="metric-card"><div class="metric-label">TRADES/MIN</div>'
                f'<div class="{tpm_c}">{tpm}</div></div>',unsafe_allow_html=True)

st.markdown("")

# Barras progresso
pc1,pc2 = st.columns(2)
with pc1:
    gp=min(1.0,max(0,stats["pnl"])/goal) if goal>0 else 0
    st.markdown(f"**🎯 Ganho: ${max(0,stats['pnl']):.2f} / Meta: ${goal:.2f} ({gp*100:.1f}%)**")
    st.progress(gp)
with pc2:
    lp=min(1.0,abs(min(0,stats["pnl"]))/stop_loss) if stop_loss>0 else 0
    ic="🔴" if lp>0.7 else ("🟡" if lp>0.4 else "🟢")
    st.markdown(f"**{ic} Perda: ${abs(min(0,stats['pnl'])):.2f} / Stop: ${stop_loss:.2f}**")
    st.progress(lp)

st.markdown("")

# ── LAYOUT PRINCIPAL ──────────────────────────────────────────────────────────
left,right = st.columns([2,1])

with left:
    st.markdown("### 📊 Trades")
    trades = manager.get_trades()
    if trades:
        df=pd.DataFrame(trades)
        df["resultado"]=df["profit"].apply(
            lambda x: f"✅ +${x:.2f}" if x>0 else f"❌ -${abs(x):.2f}")
        cols=[c for c in ["time","level","direction","stake","resultado","signal"]
              if c in df.columns]
        st.dataframe(df[cols].tail(30),use_container_width=True,hide_index=True)

        if mode in ("suicida","sniper"):
            st.markdown("#### 🔥 Últimos Níveis")
            for t in trades[-5:]:
                ic  ="✅" if t["profit"]>0 else "❌"
                col ="#00d4aa" if t["profit"]>0 else "#ff4d6d"
                after=t["stake"]+t["profit"]
                st.markdown(
                    f'<div class="level-bar">'
                    f'<span style="color:{col}">{ic} Nv{t.get("level","?")}</span> '
                    f'${t["stake"]:.2f}→<b style="color:{col}">${after:.2f}</b> '
                    f'{t["direction"]} {t["time"]}</div>',
                    unsafe_allow_html=True)
    else:
        st.info("🤖 Clica ▶ INICIAR — o bot opera sozinho.")

with right:
    st.markdown("### 🔍 Sinais")
    signals=manager.get_signals()
    if signals:
        for s in signals[-12:]:
            bc=("signal-box" if s["dir"]=="CALL"
                else "signal-box signal-sell" if s["dir"]=="PUT"
                else "signal-box signal-wait")
            ico="🟢" if s["dir"]=="CALL" else ("🔴" if s["dir"]=="PUT" else "🟡")
            st.markdown(
                f'<div class="{bc}">{ico}<b>{s["dir"]}</b> {s["time"]}<br>'
                f'<span style="color:#7c9cbf;font-size:.73rem">{s["reason"]}</span></div>',
                unsafe_allow_html=True)
    else:
        st.info("Aguardando sinais...")

    st.markdown("### 📋 Log")
    logs=manager.get_logs()
    html=""
    for e in logs[-20:]:
        cor=("#00d4aa" if any(x in e for x in ["✅","META","🎯","WIN"])
             else "#ff4d6d" if any(x in e for x in ["❌","💥","🛑","LOSS"])
             else "#ff6600" if any(x in e for x in ["🔥","SNIPER","⚡","disparar"])
             else "#f59e0b" if any(x in e for x in ["⏳","💸","📡"])
             else "#a0c0ff" if any(x in e for x in ["📝","📊","✅ Con"])
             else "#7c9cbf")
        html+=(f'<div style="font-family:JetBrains Mono,monospace;font-size:.71rem;'
               f'color:{cor};padding:2px 0;border-bottom:1px solid #0d1520">{e}</div>')
    st.markdown(
        f'<div style="background:#111827;border-radius:8px;padding:10px;'
        f'max-height:400px;overflow-y:auto">{html}</div>',
        unsafe_allow_html=True)

# ── START / STOP ──────────────────────────────────────────────────────────────
if start_btn:
    if manager.is_running():
        st.warning("⚠️ Bot já está a correr!")
    elif not api_key:
        st.error("❌ Insere o PAT Token!")
    elif not app_id:
        st.error("❌ Insere o App ID!")
    else:
        manager.reset()
        cfg={
            "api_token":         api_key,
            "app_id":            app_id,
            "account_type":      account_type,
            "symbol":            symbol,
            "contract_duration": contract_duration,
            "chart_tf":          chart_tf,
            "mode":              mode,
            "stake":             stake,
            "goal":              goal,
            "stop_loss":         stop_loss,
            "max_consec":        max_consec,
        }
        bot=SevenLevelsBot(cfg,manager)
        st.session_state.bot    =bot
        st.session_state.running=True
        threading.Thread(target=lambda:asyncio.run(bot.run()),daemon=True).start()
        lbl="🎯 SNIPER SUICIDA" if is_sniper else "Bot"
        st.success(f"✅ {lbl} iniciado! {sym_label} | Meta:+${goal:.2f}")
        time.sleep(1); st.rerun()

if stop_btn:
    if st.session_state.bot: st.session_state.bot.stop()
    manager.set_running(False,"Parado manualmente")
    st.session_state.running=False
    st.warning("⏹ Bot parado.")
    time.sleep(1); st.rerun()

if manager.is_running():
    time.sleep(2 if is_sniper else 3)
    st.rerun()
