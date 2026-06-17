# =============================================================================
#  SEVEN LEVELS BOT — MA7 + MACD + Compounding Automático
#  Estratégia: Média Móvel 7 + MACD confirmação
#  Modos: Conservador | Moderado | Compounding Suicida ($1→$1000)
#  API: Nova Deriv API (PAT → REST → OTP → WebSocket)
#  Bot 100% automático: começa, opera, para sozinho (meta ou perda)
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
#  PARES DISPONÍVEIS
# ─────────────────────────────────────────────────────────────────────────────

FOREX_PAIRS = [
    "frxEURUSD", "frxGBPUSD", "frxUSDJPY", "frxUSDCHF",
    "frxAUDUSD", "frxUSDCAD", "frxNZDUSD",
    "frxEURGBP", "frxEURJPY", "frxGBPJPY", "frxAUDJPY",
]

COMMODITIES = [
    "frxXAUUSD", "frxXAGUSD", "frxBROUSD",
]

SYMBOL_LABELS = {
    "frxEURUSD": "EUR/USD", "frxGBPUSD": "GBP/USD",
    "frxUSDJPY": "USD/JPY", "frxUSDCHF": "USD/CHF",
    "frxAUDUSD": "AUD/USD", "frxUSDCAD": "USD/CAD",
    "frxNZDUSD": "NZD/USD", "frxEURGBP": "EUR/GBP",
    "frxEURJPY": "EUR/JPY", "frxGBPJPY": "GBP/JPY",
    "frxAUDJPY": "AUD/JPY", "frxXAUUSD": "Ouro/USD 🥇",
    "frxXAGUSD": "Prata/USD 🥈", "frxBROUSD": "Petróleo Brent 🛢️",
}

ALL_SYMBOLS = FOREX_PAIRS + COMMODITIES

# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 1 — DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Candle:
    open:  float
    high:  float
    low:   float
    close: float
    epoch: int = 0

    @property
    def body(self):       return abs(self.close - self.open)
    @property
    def upper_wick(self): return self.high - max(self.open, self.close)
    @property
    def lower_wick(self): return min(self.open, self.close) - self.low
    @property
    def is_bullish(self): return self.close > self.open
    @property
    def is_bearish(self): return self.close < self.open
    @property
    def range(self):      return self.high - self.low


@dataclass
class Signal:
    direction:  str    # "CALL" | "PUT" | "WAIT"
    confidence: float
    reason:     str
    ma_score:   float = 0.0
    macd_score: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 2 — INDICADORES (MA7 + MACD)
# ─────────────────────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> list:
    if len(prices) < period:
        return prices[:]
    k   = 2 / (period + 1)
    out = [prices[0]]
    for p in prices[1:]:
        out.append(p * k + out[-1] * (1 - k))
    return out


def _sma(prices: list, period: int) -> list:
    out = []
    for i in range(len(prices)):
        if i < period - 1:
            out.append(prices[i])
        else:
            out.append(statistics.mean(prices[i-period+1:i+1]))
    return out


class MA7Indicator:
    """
    Média Móvel Simples de 7 períodos — a 'linha vermelha' da estratégia.
    Regras:
      - Preço ACIMA da MA7 = tendência de ALTA
      - Preço ABAIXO da MA7 = tendência de BAIXA
      - Entrada ideal: preço TOCA ou se APROXIMA da MA7
    """
    PERIOD = 7

    def analyze(self, closes: list) -> dict:
        if len(closes) < self.PERIOD + 2:
            return {"trend": "SIDEWAYS", "touch": False, "distance_pct": 1.0, "ma": None}

        ma  = _sma(closes, self.PERIOD)
        ma_now  = ma[-1]
        price   = closes[-1]
        dist    = abs(price - ma_now) / ma_now   # distância em %

        # Tendência pela posição do preço em relação à MA
        if price > ma_now:
            trend = "UP"
        elif price < ma_now:
            trend = "DOWN"
        else:
            trend = "SIDEWAYS"

        # "Toque" = preço dentro de 0.15% da MA (zona de entrada ideal)
        touch = dist < 0.0015

        # "Aproximação" = preço dentro de 0.35% da MA (zona de entrada boa)
        near  = dist < 0.0035

        # Score: quanto mais perto da MA, maior a pontuação
        score = max(0.0, 1.0 - dist / 0.004)

        return {
            "trend":    trend,
            "touch":    touch,
            "near":     near,
            "distance": dist,
            "score":    round(score, 2),
            "ma":       ma_now,
            "ma_list":  ma,
        }


class MACDIndicator:
    """
    MACD padrão: EMA12 - EMA26, Signal = EMA9 do MACD
    Regras:
      - MACD > Signal E crescendo = confirmação ALTA
      - MACD < Signal E descendo  = confirmação BAIXA
      - Histograma crescente/decrescente indica força
    """
    FAST   = 12
    SLOW   = 26
    SIGNAL = 9

    def analyze(self, closes: list) -> dict:
        if len(closes) < self.SLOW + self.SIGNAL + 2:
            return {"direction": "NEUTRAL", "score": 0.0, "histogram": 0.0}

        ema_fast = _ema(closes, self.FAST)
        ema_slow = _ema(closes, self.SLOW)

        macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
        signal_line = _ema(macd_line, self.SIGNAL)
        histogram   = [m - s for m, s in zip(macd_line, signal_line)]

        macd_now  = macd_line[-1]
        sig_now   = signal_line[-1]
        hist_now  = histogram[-1]
        hist_prev = histogram[-2] if len(histogram) > 1 else 0

        # Direção e força
        if macd_now > sig_now and hist_now > 0:
            direction = "BULL"
            # Histograma a crescer = confirmação mais forte
            strength  = min(1.0, abs(hist_now) / (abs(macd_now) + 1e-9) * 5)
            growing   = hist_now > hist_prev
        elif macd_now < sig_now and hist_now < 0:
            direction = "BEAR"
            strength  = min(1.0, abs(hist_now) / (abs(macd_now) + 1e-9) * 5)
            growing   = hist_now < hist_prev
        else:
            direction = "NEUTRAL"
            strength  = 0.0
            growing   = False

        score = round(strength * (1.2 if growing else 0.8), 2)
        score = min(1.0, score)

        return {
            "direction": direction,
            "score":     score,
            "histogram": hist_now,
            "growing":   growing,
            "macd":      macd_now,
            "signal":    sig_now,
        }


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 3 — SEVEN LEVELS ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class SevenLevelsEngine:
    """
    Implementação fiel da estratégia do vídeo:

    ENTRADA IDEAL (score máximo):
      → Preço TOCA a MA7
      → MACD confirma na mesma direção
      → Entrar dentro dos primeiros 30s da vela

    ENTRADA DE FLUXO (score médio):
      → Preço NÃO tocou a MA7 mas está perto
      → MACD forte + histograma crescente na mesma direção
      → Velas anteriores confirmam tendência

    BLOQUEIO:
      → MACD contra a direção da MA7
      → Mercado lateral (preço cruzando MA repetidamente)
      → Confiança abaixo do mínimo configurado
    """
    MIN_CONF_CONSERVATIVE = 0.75
    MIN_CONF_MODERATE     = 0.60
    MIN_CONF_SUICIDE      = 0.50   # suicida aceita entradas com menos confirmação

    def __init__(self, mode: str = "conservador"):
        self.mode = mode
        self.ma7  = MA7Indicator()
        self.macd = MACDIndicator()
        self.min_conf = {
            "conservador": self.MIN_CONF_CONSERVATIVE,
            "moderado":    self.MIN_CONF_MODERATE,
            "suicida":     self.MIN_CONF_SUICIDE,
        }.get(mode, self.MIN_CONF_CONSERVATIVE)

    def _count_consecutive(self, candles: list, bullish: bool) -> int:
        count = 0
        for c in reversed(candles[:-1]):
            if (bullish and c.is_bullish) or (not bullish and c.is_bearish):
                count += 1
            else:
                break
        return count

    def evaluate(self, candles: list) -> Signal:
        if len(candles) < 35:
            return Signal("WAIT", 0.0, "aguardando candles suficientes...")

        closes = [c.close for c in candles]

        # ── Indicadores ───────────────────────────────────────────────────────
        ma_result   = self.ma7.analyze(closes)
        macd_result = self.macd.analyze(closes)

        trend    = ma_result["trend"]
        ma_score = ma_result["score"]
        ma_now   = ma_result["ma"]
        price    = closes[-1]

        macd_dir   = macd_result["direction"]
        macd_score = macd_result["score"]
        macd_grow  = macd_result["growing"]

        # ── Lateral — não operar ──────────────────────────────────────────────
        if trend == "SIDEWAYS":
            return Signal("WAIT", 0.0, "MA7: mercado lateral")

        # ── Direção base pela MA7 ─────────────────────────────────────────────
        base_dir    = "CALL" if trend == "UP" else "PUT"
        macd_expect = "BULL" if base_dir == "CALL" else "BEAR"

        # ── MACD contra a MA7 — bloquear ──────────────────────────────────────
        if macd_dir not in ("NEUTRAL", macd_expect):
            return Signal("WAIT", 0.0,
                          f"MACD contra MA7: MA={trend} MACD={macd_dir} — bloqueado")

        # ── Contar velas consecutivas na direção ──────────────────────────────
        consec = self._count_consecutive(candles, base_dir == "CALL")

        # ── Calcular confiança ────────────────────────────────────────────────
        touch_bonus = 0.25 if ma_result["touch"] else (0.10 if ma_result["near"] else 0.0)
        macd_bonus  = macd_score * 0.35
        consec_bon  = min(0.20, consec * 0.05)
        grow_bonus  = 0.10 if macd_grow else 0.0

        conf = round(min(1.0, 0.30 + touch_bonus + macd_bonus + consec_bon + grow_bonus), 2)

        # ── Verificar confiança mínima ────────────────────────────────────────
        if conf < self.min_conf:
            touch_str = "TOQUE ✅" if ma_result["touch"] else (
                        "perto" if ma_result["near"] else f"longe ({ma_result['distance']*100:.2f}%)")
            return Signal("WAIT", conf,
                          f"conf baixa ({conf:.2f}<{self.min_conf}) | MA:{touch_str} | MACD:{macd_dir}({macd_score:.2f})")

        # ── Construir razão detalhada ─────────────────────────────────────────
        touch_str = "TOQUE NA MA7 ✅" if ma_result["touch"] else (
                    f"perto MA7 ({ma_result['distance']*100:.2f}%)" if ma_result["near"]
                    else f"fluxo forte ({ma_result['distance']*100:.2f}% da MA7)")
        reason = (f"7L {base_dir} | {touch_str} | MACD:{macd_dir}({macd_score:.2f})"
                  f"{' 📈grow' if macd_grow else ''} | velas:{consec} | conf:{conf:.2f}")

        return Signal(base_dir, conf, reason, ma_score, macd_score)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 4 — COMPOUNDING MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class CompoundingManager:
    """
    Gere os 3 modos de apostas:

    CONSERVADOR: aposta fixa, sem compounding
    MODERADO:    aposta cresce 20% por win, reset em loss
    SUICIDA:     reinveste TUDO a cada trade (fiel ao vídeo)
                 $1 → $2 → $4 → $8 → ... → $1000
    """

    def __init__(self, mode: str, base_stake: float, goal: float, payout_pct: float = 0.85):
        self.mode        = mode
        self.base_stake  = base_stake
        self.goal        = goal
        self.payout      = payout_pct   # % de retorno da Deriv (tipicamente 80-95%)
        self.current     = base_stake
        self.level       = 1
        self.peak        = base_stake
        self.history     = []           # histórico dos níveis

    def next_stake(self) -> float:
        return round(self.current, 2)

    def on_win(self, profit: float):
        self.history.append({"level": self.level, "stake": self.current,
                             "result": "WIN", "profit": profit})
        if self.mode == "suicida":
            # Reinveste tudo: capital + lucro
            self.current = round(self.current + profit, 2)
            self.level  += 1
            self.peak    = max(self.peak, self.current)
        elif self.mode == "moderado":
            # Cresce 20% por win
            self.current = round(min(self.current * 1.20, self.goal * 0.5), 2)
            self.level  += 1
        else:
            # Conservador: aposta fixa
            self.current = self.base_stake

    def on_loss(self, loss: float):
        self.history.append({"level": self.level, "stake": self.current,
                             "result": "LOSS", "profit": -abs(loss)})
        if self.mode == "suicida":
            # Perde tudo — reinicia do zero
            self.current = self.base_stake
            self.level   = 1
        elif self.mode == "moderado":
            # Reset para base
            self.current = self.base_stake
            self.level   = 1
        else:
            self.current = self.base_stake

    def progress_pct(self, current_balance: float) -> float:
        if self.goal <= self.base_stake: return 0.0
        return min(100.0, (current_balance - self.base_stake) / (self.goal - self.base_stake) * 100)

    def levels_info(self) -> str:
        if self.mode == "suicida":
            return f"Nível {self.level} | Próxima aposta: ${self.current:.2f} | Pico: ${self.peak:.2f}"
        return f"Aposta atual: ${self.current:.2f}"


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 5 — SESSION MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class SessionManager:
    def __init__(self):
        self._lock          = threading.Lock()
        self._trades        = []
        self._logs          = deque(maxlen=300)
        self._signals       = deque(maxlen=50)
        self._pnl           = 0.0
        self._wins          = 0
        self._losses        = 0
        self._consec_losses = 0
        self._max_consec    = 0
        self._running       = False
        self._stop_reason   = ""

    def set_running(self, v: bool, reason: str = ""):
        with self._lock:
            self._running     = v
            self._stop_reason = reason

    def is_running(self):
        with self._lock: return self._running

    def stop_reason(self):
        with self._lock: return self._stop_reason

    def add_trade(self, symbol, direction, stake, profit, level=1, signal_reason=""):
        with self._lock:
            entry = {
                "time":      datetime.now().strftime("%H:%M:%S"),
                "symbol":    SYMBOL_LABELS.get(symbol, symbol),
                "direction": direction,
                "level":     level,
                "stake":     round(stake, 2),
                "profit":    round(profit, 2),
                "signal":    signal_reason[:60],
            }
            self._trades.append(entry)
            self._pnl += profit
            if profit > 0:
                self._wins         += 1
                self._consec_losses = 0
            else:
                self._losses       += 1
                self._consec_losses += 1
                self._max_consec    = max(self._max_consec, self._consec_losses)
            r = f"✅ +${profit:.2f}" if profit > 0 else f"❌ -${abs(profit):.2f}"
            self._logs.append(
                f"[{entry['time']}] Nv{level} {direction} {entry['symbol']} "
                f"stake=${stake:.2f} {r}")

    def add_signal(self, direction, reason):
        with self._lock:
            self._signals.append({
                "time":   datetime.now().strftime("%H:%M:%S"),
                "dir":    direction,
                "reason": reason,
            })

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

    def stats(self):
        with self._lock:
            total   = self._wins + self._losses
            winrate = (self._wins / total * 100) if total > 0 else 0.0
            return {
                "pnl":          round(self._pnl, 2),
                "trades":       total,
                "wins":         self._wins,
                "losses":       self._losses,
                "winrate":      winrate,
                "consec_losses": self._consec_losses,
                "max_consec":   self._max_consec,
            }

    def reset(self):
        with self._lock:
            self._trades = []; self._pnl = 0.0
            self._wins = 0; self._losses = 0
            self._consec_losses = 0; self._max_consec = 0
            self._signals.clear(); self._logs.clear()
            self._running = False; self._stop_reason = ""


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 6 — DERIV CLIENT
# ─────────────────────────────────────────────────────────────────────────────

DERIV_REST_BASE = "https://api.derivws.com"

class DerivClient:
    def __init__(self, pat_token, app_id, account_type="demo"):
        self.pat            = pat_token
        self.app_id         = app_id
        self.account_type   = account_type
        self._ws            = None
        self._req_id        = 1
        self._pending       = {}
        self._candles_q     = asyncio.Queue(maxsize=2000)
        self._listener_task = None
        self._account_id    = None

    def _headers(self):
        return {"Authorization": f"Bearer {self.pat}",
                "Deriv-App-ID":  self.app_id,
                "Content-Type":  "application/json"}

    async def _get_account_id(self):
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=self._headers()) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise PermissionError(f"Erro contas: {body}")
                for acc in body.get("data", []):
                    if (acc.get("account_type") == self.account_type
                            and acc.get("status") == "active"):
                        return acc["account_id"]
                if self.account_type == "demo":
                    return await self._create_demo_account()
                raise RuntimeError(f"Conta '{self.account_type}' não encontrada.")

    async def _create_demo_account(self):
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=self._headers(),
                              json={"currency":"USD","group":"row",
                                    "account_type":"demo"}) as resp:
                body = await resp.json()
                if resp.status not in (200,201):
                    raise RuntimeError(f"Erro criar demo: {body}")
                return body["data"]["account_id"]

    async def _get_ws_url(self, account_id):
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts/{account_id}/otp"
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=self._headers()) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise PermissionError(f"Erro OTP: {body}")
                ws_url = body.get("data", {}).get("url")
                if not ws_url:
                    raise RuntimeError(f"URL WS não encontrado: {body}")
                return ws_url

    async def connect(self, retries=3):
        last_err = None
        for attempt in range(1, retries+1):
            try:
                self._account_id = await self._get_account_id()
                ws_url = await self._get_ws_url(self._account_id)
                self._ws = await websockets.connect(
                    ws_url, ping_interval=30, ping_timeout=10, close_timeout=5)
                self._listener_task = asyncio.create_task(self._listener())
                return
            except PermissionError: raise
            except Exception as e:
                last_err = e
                if attempt < retries: await asyncio.sleep(3*attempt)
        raise ConnectionError(f"Falha após {retries} tentativas: {last_err}")

    async def disconnect(self):
        if self._listener_task: self._listener_task.cancel()
        if self._ws:
            try: await self._ws.close()
            except: pass

    async def _send(self, payload, timeout=20.0):
        req_id = self._req_id; self._req_id += 1
        payload["req_id"] = req_id
        fut = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut
        await self._ws.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise TimeoutError(f"Request {req_id} timeout")

    async def _listener(self):
        try:
            async for raw in self._ws:
                msg    = json.loads(raw)
                req_id = msg.get("req_id")
                if req_id and req_id in self._pending:
                    fut = self._pending.pop(req_id)
                    if not fut.done(): fut.set_result(msg)
                elif msg.get("msg_type") == "ohlc":
                    await self._candles_q.put(msg)
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass

    async def subscribe_candles(self, symbol, granularity=60):
        resp = await self._send({
            "ticks_history": symbol, "style": "candles",
            "granularity": granularity, "count": 100,
            "end": "latest", "subscribe": 1})
        if resp.get("error"):
            raise RuntimeError(resp["error"]["message"])
        return resp.get("candles", [])

    async def get_candle_update(self, timeout=120.0):
        return await asyncio.wait_for(self._candles_q.get(), timeout)

    async def buy_contract(self, symbol, direction, stake, duration, duration_unit="m"):
        proposal = await self._send({
            "proposal": 1, "amount": stake, "basis": "stake",
            "contract_type": direction, "currency": "USD",
            "duration": duration, "duration_unit": duration_unit,
            "underlying_symbol": symbol})
        if proposal.get("error"):
            raise RuntimeError(proposal["error"]["message"])
        buy = await self._send({
            "buy": proposal["proposal"]["id"], "price": stake})
        if buy.get("error"):
            raise RuntimeError(buy["error"]["message"])
        return buy["buy"]

    async def get_contract_result(self, contract_id, max_wait=300.0):
        deadline = asyncio.get_event_loop().time() + max_wait
        while asyncio.get_event_loop().time() < deadline:
            resp = await self._send({
                "proposal_open_contract": 1,
                "contract_id": contract_id})
            poc = resp.get("proposal_open_contract", {})
            if poc.get("is_sold") or poc.get("status") in ("sold","won","lost"):
                return {
                    "profit": float(poc.get("profit", 0)),
                    "status": poc.get("status"),
                }
            await asyncio.sleep(3)
        raise TimeoutError("Contrato não liquidou a tempo")

    async def get_balance(self):
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=self._headers()) as resp:
                body = await resp.json()
                for acc in body.get("data", []):
                    if acc.get("account_id") == self._account_id:
                        return float(acc.get("balance", 0))
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 7 — BOT PRINCIPAL (100% automático)
# ─────────────────────────────────────────────────────────────────────────────

_DURATION_MAP = {
    "1m":  (1,"m"),  "5m":  (5,"m"),
    "15m": (15,"m"), "30m": (30,"m"),
    "1h":  (1,"h"),
}
_GRANULARITY_MAP = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
}


class SevenLevelsBot:
    """
    Bot 100% automático.
    Começa quando clicas ▶ Iniciar.
    Para sozinho quando:
      - Atinge a meta (goal)
      - Perde o stop loss
      - Atinge N perdas consecutivas
      - Modo suicida: reinicia do base_stake após loss e continua
    """

    def __init__(self, config: dict, manager: SessionManager):
        self.cfg        = config
        self.manager    = manager
        self._stop      = False

        # Configurações
        self.symbol     = config["symbol"]
        self.mode       = config["mode"]          # conservador | moderado | suicida
        self.dur        = config["duration"]
        self.dur_val, self.dur_unit = _DURATION_MAP.get(self.dur, (5,"m"))
        self.granularity = _GRANULARITY_MAP.get(self.dur, 300)

        # Capital
        self.base_stake  = float(config["stake"])
        self.goal        = float(config["goal"])
        self.stop_loss   = float(config["stop_loss"])
        self.max_consec  = int(config["max_consec"])

        # Engine e compounding
        self.engine   = SevenLevelsEngine(mode=self.mode)
        self.compound = CompoundingManager(
            mode=self.mode,
            base_stake=self.base_stake,
            goal=self.goal)

        self.client   = DerivClient(
            config["api_token"], config["app_id"],
            config.get("account_type","demo"))

        self.candles      = []
        self.total_pnl    = 0.0
        self.start_balance = 0.0

    def stop(self): self._stop = True

    async def run(self):
        label = SYMBOL_LABELS.get(self.symbol, self.symbol)
        self.manager.set_running(True)
        self.manager.log(f"🚀 Seven Levels Bot INICIADO")
        self.manager.log(f"📌 {label} | Modo: {self.mode.upper()} | TF: {self.dur}")
        self.manager.log(f"💰 Stake inicial: ${self.base_stake} | Meta: ${self.goal} | Stop: ${self.stop_loss}")

        try:
            await self.client.connect()
            self.start_balance = await self.client.get_balance()
            self.manager.log(
                f"✅ Conectado | {self.client._account_id} | "
                f"Saldo: ${self.start_balance:.2f}")

            # Carregar candles históricos
            raw = await self.client.subscribe_candles(self.symbol, self.granularity)
            for r in raw:
                self.candles.append(Candle(
                    float(r["open"]), float(r["high"]),
                    float(r["low"]),  float(r["close"]),
                    int(r.get("epoch", 0))))
            self.candles = self.candles[-150:]
            self.manager.log(f"📊 {len(self.candles)} candles carregados")
            self.manager.log(f"👁️ A monitorizar... aguardando sinal MA7+MACD")

            # ── Loop principal ─────────────────────────────────────────────────
            while not self._stop:
                stats = self.manager.stats()

                # ── Verificar condições de paragem ─────────────────────────────
                current_balance = self.start_balance + self.total_pnl

                if current_balance >= self.goal:
                    reason = f"🎯 META ATINGIDA! Saldo: ${current_balance:.2f} / Meta: ${self.goal:.2f}"
                    self.manager.log(reason)
                    self.manager.set_running(False, reason)
                    break

                if self.total_pnl <= -self.stop_loss:
                    reason = f"🛑 STOP LOSS! Perda: ${abs(self.total_pnl):.2f} / Limite: ${self.stop_loss:.2f}"
                    self.manager.log(reason)
                    self.manager.set_running(False, reason)
                    break

                if stats["consec_losses"] >= self.max_consec and self.mode != "suicida":
                    reason = f"🛑 {stats['consec_losses']} PERDAS CONSECUTIVAS — Bot parado!"
                    self.manager.log(reason)
                    self.manager.set_running(False, reason)
                    break

                # ── Aguardar novo candle ───────────────────────────────────────
                try:
                    msg  = await self.client.get_candle_update(timeout=180)
                    ohlc = msg.get("ohlc", {})
                    if ohlc:
                        c = Candle(
                            float(ohlc["open"]), float(ohlc["high"]),
                            float(ohlc["low"]),  float(ohlc["close"]),
                            int(ohlc.get("epoch", 0)))
                        if not self.candles or c.epoch != self.candles[-1].epoch:
                            self.candles.append(c)
                            if len(self.candles) > 150:
                                self.candles = self.candles[-150:]
                except asyncio.TimeoutError:
                    self.manager.log("⏳ Aguardando candle...")
                    continue

                if len(self.candles) < 35:
                    continue

                # ── Avaliar sinal ──────────────────────────────────────────────
                signal = self.engine.evaluate(self.candles)
                self.manager.add_signal(signal.direction, signal.reason)

                if signal.direction == "WAIT":
                    continue

                # ── Log do sinal ───────────────────────────────────────────────
                stake = self.compound.next_stake()
                self.manager.log(
                    f"📡 SINAL {signal.direction} | conf={signal.confidence:.2f} "
                    f"| MA={signal.ma_score:.2f} | MACD={signal.macd_score:.2f}")
                self.manager.log(f"   {signal.reason}")
                self.manager.log(
                    f"💸 Nível {self.compound.level} | Apostando ${stake:.2f}")

                # ── Executar trade ─────────────────────────────────────────────
                try:
                    buy_info    = await self.client.buy_contract(
                        self.symbol, signal.direction,
                        stake, self.dur_val, self.dur_unit)
                    contract_id = buy_info.get("contract_id")
                    self.manager.log(f"📝 Contrato #{contract_id} aberto")

                    result = await self.client.get_contract_result(contract_id)
                    profit = result["profit"]
                    self.total_pnl += profit

                    self.manager.add_trade(
                        self.symbol, signal.direction,
                        stake, profit,
                        self.compound.level,
                        signal.reason[:60])

                    if profit > 0:
                        self.compound.on_win(profit)
                        new_balance = self.start_balance + self.total_pnl
                        self.manager.log(
                            f"✅ WIN +${profit:.2f} | Saldo: ${new_balance:.2f} "
                            f"| Próx aposta: ${self.compound.current:.2f}")
                        # Modo suicida: mostra progresso
                        if self.mode == "suicida":
                            self.manager.log(
                                f"🔥 Nível {self.compound.level} | "
                                f"Acumulado: ${self.compound.current:.2f} / Meta: ${self.goal:.2f}")
                    else:
                        self.compound.on_loss(abs(profit))
                        new_balance = self.start_balance + self.total_pnl
                        self.manager.log(
                            f"❌ LOSS -${abs(profit):.2f} | Saldo: ${new_balance:.2f}")
                        if self.mode == "suicida":
                            self.manager.log(
                                f"💀 Suicida: perdeu tudo! Reiniciando com ${self.base_stake:.2f}")

                    await asyncio.sleep(3)

                except Exception as e:
                    self.manager.log(f"❌ Erro no trade: {e}")
                    await asyncio.sleep(10)

        except Exception as e:
            self.manager.log(f"💥 Erro crítico: {e}")
            self.manager.set_running(False, f"Erro: {e}")
        finally:
            await self.client.disconnect()
            final_balance = self.start_balance + self.total_pnl
            self.manager.log(
                f"🔌 Bot encerrado | P&L total: ${self.total_pnl:.2f} "
                f"| Saldo final: ${final_balance:.2f}")
            if self.manager.is_running():
                self.manager.set_running(False, "Bot encerrado")


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 8 — STREAMLIT DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Seven Levels Bot", page_icon="🎯",
    layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;700&display=swap');
html,body,[class*="css"]{ font-family:'Space Grotesk',sans-serif; }
.stApp{ background:#0a0e1a; color:#e0e6f5; }
.metric-card{ background:#111827; border:1px solid #1e3a5f; border-radius:12px; padding:16px; text-align:center; }
.metric-label{ font-size:.72rem; color:#7c9cbf; margin-bottom:4px; }
.profit { color:#00d4aa; font-family:'JetBrains Mono',monospace; font-size:1.6rem; font-weight:700; }
.loss   { color:#ff4d6d; font-family:'JetBrains Mono',monospace; font-size:1.6rem; font-weight:700; }
.neutral{ color:#7c9cbf; font-family:'JetBrains Mono',monospace; font-size:1.6rem; font-weight:700; }
.warn   { color:#f59e0b; font-family:'JetBrains Mono',monospace; font-size:1.6rem; font-weight:700; }
.mode-card{ border-radius:10px; padding:14px 18px; margin:6px 0; cursor:pointer; }
.mode-conservador{ background:#0d2818; border:2px solid #00d4aa; }
.mode-moderado   { background:#1a1a0d; border:2px solid #f59e0b; }
.mode-suicida    { background:#200a0a; border:2px solid #ff4d6d; }
.signal-box { background:#111827; border-left:4px solid #00d4aa; border-radius:8px; padding:10px 14px; margin:4px 0; font-family:'JetBrains Mono',monospace; font-size:.80rem; }
.signal-sell{ border-left-color:#ff4d6d; }
.signal-wait{ border-left-color:#f59e0b; }
.level-bar  { background:#111827; border:1px solid #1e3a5f; border-radius:8px; padding:12px; margin:4px 0; }
.dot-green{ width:10px;height:10px;background:#00d4aa;border-radius:50%;display:inline-block;margin-right:6px; }
.dot-red  { width:10px;height:10px;background:#ff4d6d;border-radius:50%;display:inline-block;margin-right:6px; }
.dot-yellow{width:10px;height:10px;background:#f59e0b;border-radius:50%;display:inline-block;margin-right:6px; }
.stButton>button{ background:linear-gradient(135deg,#00d4aa,#0099ff); color:#0a0e1a; font-weight:700; border:none; border-radius:8px; font-size:1rem; padding:10px; }
.stop-btn>button{ background:linear-gradient(135deg,#ff4d6d,#c0392b) !important; }
.banner-running{ background:linear-gradient(135deg,#003320,#001a10); border:1px solid #00d4aa; border-radius:10px; padding:14px 20px; margin:10px 0; }
.banner-stopped{ background:linear-gradient(135deg,#200a0a,#100505); border:1px solid #ff4d6d; border-radius:10px; padding:14px 20px; margin:10px 0; }
.banner-goal   { background:linear-gradient(135deg,#1a3300,#0d1a00); border:1px solid #00ff88; border-radius:10px; padding:14px 20px; margin:10px 0; text-align:center; }
</style>
""", unsafe_allow_html=True)

# Session state
for k, v in [("bot", None), ("running", False), ("manager", SessionManager()),
              ("mode", "conservador"), ("compound_mgr", None)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🎯 Seven Levels Bot")
    st.markdown("*MA7 + MACD — Automático*")
    st.markdown("---")

    api_key = st.text_input("🔑 PAT Token", type="password",
                             value=os.environ.get("DERIV_API_TOKEN",""))
    app_id  = st.text_input("🆔 App ID",
                             value=os.environ.get("DERIV_APP_ID",""))

    st.markdown("---")
    st.markdown("### 🌍 Mercado")
    account_type = st.selectbox("Conta", ["demo","real"])

    cat = st.radio("Categoria", ["Forex", "Commodities"])
    syms = FOREX_PAIRS if cat == "Forex" else COMMODITIES
    symbol = st.selectbox("Par", syms,
                           format_func=lambda x: SYMBOL_LABELS.get(x, x))

    duration = st.selectbox("Timeframe", ["1m","5m","15m","30m","1h"],
                             index=1,
                             help="1m = mais trades, mais risco | 15m = mais fiável")

    st.markdown("---")
    st.markdown("### 🎮 Modo de Operação")

    mode = st.radio("", ["conservador","moderado","suicida"],
                    format_func=lambda x: {
                        "conservador": "🟢 Conservador — Aposta fixa",
                        "moderado":    "🟡 Moderado — Cresce 20% por win",
                        "suicida":     "🔴 Suicida — Reinveste tudo ($1→$1000)",
                    }[x])

    if mode == "conservador":
        st.markdown('<div class="mode-card mode-conservador">Aposta fixa em todos os trades. Para após N perdas consecutivas. Mais seguro.</div>', unsafe_allow_html=True)
    elif mode == "moderado":
        st.markdown('<div class="mode-card mode-moderado">Aposta cresce 20% em cada win. Reset em loss. Risco médio.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="mode-card mode-suicida">⚠️ Reinveste TODO o capital a cada trade. Uma única perda reinicia do início. Exactamente como no vídeo. ALTO RISCO.</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 💰 Capital")

    if mode == "suicida":
        stake     = st.number_input("Aposta inicial ($)", min_value=0.35, max_value=10.0,
                                     value=1.0, step=0.35,
                                     help="Começa aqui — reinveste tudo até à meta")
        goal      = st.number_input("Meta ($)", min_value=10.0, max_value=10000.0,
                                     value=1000.0, step=50.0)
        stop_loss = st.number_input("Stop loss ($)", min_value=0.35, max_value=100.0,
                                     value=stake, step=0.35,
                                     help="No suicida = valor inicial perdido se falhar")
        max_consec = 999  # suicida nunca para por consecutivas — reinicia e tenta de novo
    else:
        stake      = st.number_input("Aposta ($)", min_value=0.35, max_value=500.0,
                                      value=1.0, step=0.5)
        goal       = st.number_input("Meta ($)", min_value=1.0, max_value=10000.0,
                                      value=10.0, step=1.0)
        stop_loss  = st.number_input("Stop loss ($)", min_value=0.35, max_value=1000.0,
                                      value=5.0, step=0.5)
        max_consec = st.number_input("Stop por perdas consecutivas",
                                      min_value=1, max_value=10, value=3)

    st.markdown("---")
    c1, c2 = st.columns(2)
    start_btn = c1.button("▶ INICIAR", use_container_width=True)
    with c2:
        st.markdown('<div class="stop-btn">', unsafe_allow_html=True)
        stop_btn = st.button("⏹ PARAR", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("# 🎯 Seven Levels Bot")
st.markdown(f"*MA7 + MACD · {SYMBOL_LABELS.get(symbol, symbol)} · {duration} · Modo: **{mode.upper()}***")

manager = st.session_state.manager
is_running = manager.is_running()
stop_reason = manager.stop_reason()

if is_running:
    st.markdown(f'<div class="banner-running"><span class="dot-green"></span>'
                f'<b>BOT A OPERAR AUTOMATICAMENTE</b> — '
                f'{SYMBOL_LABELS.get(symbol,symbol)} | {mode.upper()}</div>',
                unsafe_allow_html=True)
elif stop_reason and "META" in stop_reason:
    st.markdown(f'<div class="banner-goal">🏆 <b>{stop_reason}</b></div>',
                unsafe_allow_html=True)
elif stop_reason:
    st.markdown(f'<div class="banner-stopped"><span class="dot-red"></span>'
                f'<b>BOT PARADO:</b> {stop_reason}</div>',
                unsafe_allow_html=True)
else:
    st.markdown(f'<div class="banner-stopped"><span class="dot-red"></span>'
                f'<b>BOT OFFLINE</b> — Clica ▶ INICIAR para começar</div>',
                unsafe_allow_html=True)

# ── Métricas ──────────────────────────────────────────────────────────────────
stats = manager.stats()

m1,m2,m3,m4,m5,m6 = st.columns(6)
with m1:
    cls = "profit" if stats["pnl"] >= 0 else "loss"
    st.markdown(f'<div class="metric-card"><div class="metric-label">P&L TOTAL</div>'
                f'<div class="{cls}">${stats["pnl"]:.2f}</div></div>', unsafe_allow_html=True)
with m2:
    st.markdown(f'<div class="metric-card"><div class="metric-label">TRADES</div>'
                f'<div class="neutral">{stats["trades"]}</div></div>', unsafe_allow_html=True)
with m3:
    wc = "profit" if stats["winrate"]>=60 else ("neutral" if stats["winrate"]>=50 else "loss")
    st.markdown(f'<div class="metric-card"><div class="metric-label">WIN RATE</div>'
                f'<div class="{wc}">{stats["winrate"]:.1f}%</div></div>', unsafe_allow_html=True)
with m4:
    st.markdown(f'<div class="metric-card"><div class="metric-label">WINS</div>'
                f'<div class="profit">{stats["wins"]}</div></div>', unsafe_allow_html=True)
with m5:
    st.markdown(f'<div class="metric-card"><div class="metric-label">LOSSES</div>'
                f'<div class="loss">{stats["losses"]}</div></div>', unsafe_allow_html=True)
with m6:
    cl   = stats["consec_losses"]
    cl_c = "loss" if cl >= max_consec else ("warn" if cl >= max_consec-1 else "neutral")
    st.markdown(f'<div class="metric-card"><div class="metric-label">CONS.LOSS</div>'
                f'<div class="{cl_c}">{cl}</div></div>', unsafe_allow_html=True)

st.markdown("")

# ── Barras de progresso ───────────────────────────────────────────────────────
prog_col1, prog_col2 = st.columns(2)
with prog_col1:
    gp = min(1.0, max(0, stats["pnl"]) / goal) if goal > 0 else 0
    st.markdown(f"**🎯 Meta: ${max(0,stats['pnl']):.2f} / ${goal:.2f} ({gp*100:.1f}%)**")
    st.progress(gp)
with prog_col2:
    lp = min(1.0, abs(min(0,stats["pnl"])) / stop_loss) if stop_loss > 0 else 0
    lc = "🔴" if lp > 0.7 else ("🟡" if lp > 0.4 else "🟢")
    st.markdown(f"**{lc} Stop Loss: ${abs(min(0,stats['pnl'])):.2f} / ${stop_loss:.2f}**")
    st.progress(lp)

st.markdown("")

# ── Layout principal ──────────────────────────────────────────────────────────
left, right = st.columns([2,1])

with left:
    st.markdown("### 📊 Trades")
    trades = manager.get_trades()
    if trades:
        df = pd.DataFrame(trades)
        df["resultado"] = df["profit"].apply(
            lambda x: f"✅ +${x:.2f}" if x > 0 else f"❌ -${abs(x):.2f}")
        cols = [c for c in ["time","level","symbol","direction","stake","resultado","signal"]
                if c in df.columns]
        st.dataframe(df[cols].tail(25), use_container_width=True, hide_index=True)

        if mode == "suicida" and len(trades) > 0:
            st.markdown("#### 🔥 Progresso dos Níveis (Suicida)")
            for t in trades[-7:]:
                icon   = "✅" if t["profit"] > 0 else "❌"
                color  = "#00d4aa" if t["profit"] > 0 else "#ff4d6d"
                st.markdown(
                    f'<div class="level-bar">'
                    f'<span style="color:{color}">{icon}</span> '
                    f'<b>Nível {t.get("level","?")}:</b> '
                    f'${t["stake"]:.2f} → '
                    f'<span style="color:{color}">${t["stake"]+t["profit"]:.2f}</span> '
                    f'({t["direction"]} | {t["time"]})'
                    f'</div>',
                    unsafe_allow_html=True)
    else:
        st.info("🤖 Nenhum trade ainda. Clica ▶ INICIAR e o bot opera sozinho.")

with right:
    st.markdown("### 🔍 Sinais (MA7 + MACD)")
    signals = manager.get_signals()
    if signals:
        for s in signals[-10:]:
            bc  = ("signal-box" if s["dir"]=="CALL"
                   else "signal-box signal-sell" if s["dir"]=="PUT"
                   else "signal-box signal-wait")
            ico = "🟢" if s["dir"]=="CALL" else ("🔴" if s["dir"]=="PUT" else "🟡")
            st.markdown(
                f'<div class="{bc}">{ico} <b>{s["dir"]}</b> {s["time"]}<br>'
                f'<span style="color:#7c9cbf;font-size:.75rem">{s["reason"]}</span></div>',
                unsafe_allow_html=True)
    else:
        st.info("Aguardando sinais MA7+MACD...")

    st.markdown("### 📋 Log ao Vivo")
    logs = manager.get_logs()
    html = ""
    for e in logs[-16:]:
        cor = ("#00d4aa" if "✅" in e or "META" in e or "🎯" in e
               else "#ff4d6d" if "❌" in e or "💥" in e or "🛑" in e or "💀" in e
               else "#f59e0b" if "⏳" in e or "📡" in e or "🔥" in e
               else "#a0c0ff" if "📝" in e or "📊" in e
               else "#7c9cbf")
        html += (f'<div style="font-family:JetBrains Mono,monospace;font-size:.73rem;'
                 f'color:{cor};padding:2px 0;border-bottom:1px solid #0d1520">{e}</div>')
    st.markdown(
        f'<div style="background:#111827;border-radius:8px;padding:12px;'
        f'max-height:350px;overflow-y:auto">{html}</div>',
        unsafe_allow_html=True)

# ── Start / Stop ──────────────────────────────────────────────────────────────
if start_btn:
    if manager.is_running():
        st.warning("⚠️ Bot já está a correr!")
    elif not api_key:
        st.error("❌ Insere o PAT Token!")
    elif not app_id:
        st.error("❌ Insere o App ID!")
    else:
        manager.reset()
        cfg = {
            "api_token":    api_key,
            "app_id":       app_id,
            "account_type": account_type,
            "symbol":       symbol,
            "duration":     duration,
            "mode":         mode,
            "stake":        stake,
            "goal":         goal,
            "stop_loss":    stop_loss,
            "max_consec":   max_consec,
        }
        bot = SevenLevelsBot(cfg, manager)
        st.session_state.bot     = bot
        st.session_state.running = True
        threading.Thread(
            target=lambda: asyncio.run(bot.run()),
            daemon=True).start()

        label = SYMBOL_LABELS.get(symbol, symbol)
        st.success(f"✅ Bot iniciado! {label} | {mode.upper()} | Meta: ${goal:.2f}")
        time.sleep(1)
        st.rerun()

if stop_btn:
    if st.session_state.bot:
        st.session_state.bot.stop()
    manager.set_running(False, "Parado manualmente pelo utilizador")
    st.session_state.running = False
    st.warning("⏹ Bot parado manualmente.")
    time.sleep(1)
    st.rerun()

# Auto-refresh quando o bot está a correr
if manager.is_running():
    time.sleep(3)
    st.rerun()
