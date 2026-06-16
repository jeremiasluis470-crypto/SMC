# =============================================================================
#  DERIV BOT PRO — arquivo único
#  5 Estratégias seleccionáveis: Precisão | S/R | Candles | Fibonacci | Smart Money
#  API: Nova Deriv API (PAT → REST → OTP → WebSocket)
#  v2 — SMC com filtro Trend obrigatório + stop por perdas consecutivas
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
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import websockets

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
    def is_doji(self):    return self.body < (self.high - self.low) * 0.1
    @property
    def range(self):      return self.high - self.low


@dataclass
class Signal:
    direction:    str    # "CALL" | "PUT" | "WAIT"
    confidence:   float
    reason:       str
    trend_score:  float = 0.0
    sr_score:     float = 0.0
    candle_score: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 2 — TREND ANALYZER
# ─────────────────────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> list:
    k   = 2 / (period + 1)
    out = [prices[0]]
    for p in prices[1:]:
        out.append(p * k + out[-1] * (1 - k))
    return out


class TrendAnalyzer:
    FAST = 8; SLOW = 21; MIN_LEN = 25

    def analyze(self, closes: list) -> tuple:
        if len(closes) < self.MIN_LEN:
            return "SIDEWAYS", 0.0
        fast  = _ema(closes, self.FAST)
        slow  = _ema(closes, self.SLOW)
        diff  = fast[-1] - slow[-1]
        slope = (slow[-1] - slow[-5]) / slow[-5] * 100
        if diff > 0 and slope > 0.02:
            score = min(1.0, abs(diff / slow[-1]) * 500 + slope * 10)
            return "UP", round(score, 2)
        elif diff < 0 and slope < -0.02:
            score = min(1.0, abs(diff / slow[-1]) * 500 + abs(slope) * 10)
            return "DOWN", round(score, 2)
        return "SIDEWAYS", round(abs(diff / slow[-1]) * 200, 2)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 3 — SUPPORT & RESISTANCE
# ─────────────────────────────────────────────────────────────────────────────

class SupportResistance:
    CLUSTER_PCT = 0.0015

    def get_levels(self, candles: list) -> dict:
        if len(candles) < 5:
            return {"supports": [], "resistances": []}
        h  = max(c.high  for c in candles[-5:])
        l  = min(c.low   for c in candles[-5:])
        c_ = candles[-1].close
        P  = (h + l + c_) / 3
        r1, r2 = 2*P - l, P + (h - l)
        s1, s2 = 2*P - h, P - (h - l)
        highs = [c.high for c in candles[-20:]]
        lows  = [c.low  for c in candles[-20:]]
        resistances = sorted(set(self._cluster(highs) + [r1, r2]), reverse=True)[:5]
        supports    = sorted(set(self._cluster(lows)  + [s1, s2]))[:5]
        return {"supports": supports, "resistances": resistances, "pivot": P}

    def _cluster(self, prices: list) -> list:
        if not prices: return []
        avg = statistics.mean(prices)
        tol = avg * self.CLUSTER_PCT
        clusters, used = [], [False] * len(prices)
        for i, p in enumerate(prices):
            if used[i]: continue
            group = [p]
            for j in range(i + 1, len(prices)):
                if abs(prices[j] - p) <= tol:
                    group.append(prices[j]); used[j] = True
            clusters.append(statistics.mean(group))
        return clusters

    def score(self, price: float, levels: dict) -> tuple:
        if not levels.get("supports") or not levels.get("resistances"):
            return "WAIT", 0.0
        near_sup = min(abs(price - s) / price for s in levels["supports"])
        near_res = min(abs(price - r) / price for r in levels["resistances"])
        threshold = 0.002
        if near_sup < threshold and near_sup < near_res:
            return "CALL", round(max(0.0, 1.0 - near_sup / threshold), 2)
        elif near_res < threshold and near_res < near_sup:
            return "PUT",  round(max(0.0, 1.0 - near_res / threshold), 2)
        return "WAIT", 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 4 — CANDLE BIBLE (18 padrões)
# ─────────────────────────────────────────────────────────────────────────────

class CandleBible:
    def analyze(self, candles: list) -> tuple:
        if len(candles) < 3:
            return "WAIT", 0.0, "dados insuficientes"
        c0, c1, c2 = candles[-3], candles[-2], candles[-1]
        avg_range = statistics.mean(c.range for c in candles[-10:]) if len(candles) >= 10 else c2.range

        checks = [
            (c2.lower_wick >= 2*c2.body and c2.upper_wick <= 0.3*c2.body and c2.body > 0,
             "CALL", 0.75, "Hammer"),
            (c2.upper_wick >= 2*c2.body and c2.lower_wick <= 0.3*c2.body and c2.body > 0,
             "PUT",  0.75, "Shooting Star"),
            (c2.is_doji and c2.lower_wick >= (c2.high - c2.low) * 0.3,
             "CALL", 0.65, "Dragonfly Doji"),
            (c2.is_doji and c2.upper_wick >= (c2.high - c2.low) * 0.3,
             "PUT",  0.65, "Gravestone Doji"),
            (c2.is_bullish and c2.body > avg_range*0.8 and c2.upper_wick < c2.body*0.1 and c2.lower_wick < c2.body*0.1,
             "CALL", 0.80, "Bullish Marubozu"),
            (c2.is_bearish and c2.body > avg_range*0.8 and c2.upper_wick < c2.body*0.1 and c2.lower_wick < c2.body*0.1,
             "PUT",  0.80, "Bearish Marubozu"),
            (c1.is_bearish and c2.is_bullish and c2.open < c1.close and c2.close > c1.open,
             "CALL", 0.85, "Bullish Engulfing"),
            (c1.is_bullish and c2.is_bearish and c2.open > c1.close and c2.close < c1.open,
             "PUT",  0.85, "Bearish Engulfing"),
            (c1.is_bearish and c2.is_bullish and c2.open > c1.close and c2.close < c1.open,
             "CALL", 0.65, "Bullish Harami"),
            (c1.is_bullish and c2.is_bearish and c2.open < c1.close and c2.close > c1.open,
             "PUT",  0.65, "Bearish Harami"),
            (c1.is_bearish and c2.is_bullish and abs(c1.low - c2.low) < avg_range*0.05,
             "CALL", 0.70, "Tweezer Bottom"),
            (c1.is_bullish and c2.is_bearish and abs(c1.high - c2.high) < avg_range*0.05,
             "PUT",  0.70, "Tweezer Top"),
            (c1.is_bearish and c2.is_bullish and c2.open < c1.low and c2.close > (c1.open+c1.close)/2,
             "CALL", 0.78, "Piercing Pattern"),
            (c1.is_bullish and c2.is_bearish and c2.open > c1.high and c2.close < (c1.open+c1.close)/2,
             "PUT",  0.78, "Dark Cloud Cover"),
            (c0.is_bearish and c1.body < c0.body*0.3 and c2.is_bullish and c2.close > (c0.open+c0.close)/2,
             "CALL", 0.88, "Morning Star"),
            (c0.is_bullish and c1.body < c0.body*0.3 and c2.is_bearish and c2.close < (c0.open+c0.close)/2,
             "PUT",  0.88, "Evening Star"),
            (c0.is_bullish and c1.is_bullish and c2.is_bullish and
             c1.close > c0.close and c2.close > c1.close and
             c0.body > avg_range*0.4 and c1.body > avg_range*0.4 and c2.body > avg_range*0.4,
             "CALL", 0.90, "Three White Soldiers"),
            (c0.is_bearish and c1.is_bearish and c2.is_bearish and
             c1.close < c0.close and c2.close < c1.close and
             c0.body > avg_range*0.4 and c1.body > avg_range*0.4 and c2.body > avg_range*0.4,
             "PUT",  0.90, "Three Black Crows"),
        ]
        for cond, direction, conf, name in checks:
            if cond:
                return direction, conf, name
        return "WAIT", 0.0, "sem padrao"


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 5 — FIBONACCI ANALYZER
# ─────────────────────────────────────────────────────────────────────────────

class FibonacciAnalyzer:
    LEVELS = [0.0, 0.236, 0.382, 0.500, 0.618, 0.786, 1.0]

    def analyze(self, candles: list) -> tuple:
        if len(candles) < 20:
            return "WAIT", 0.0, "candles insuficientes"
        highs      = [c.high for c in candles[-20:]]
        lows       = [c.low  for c in candles[-20:]]
        swing_high = max(highs)
        swing_low  = min(lows)
        price      = candles[-1].close
        diff       = swing_high - swing_low
        if diff == 0:
            return "WAIT", 0.0, "range zero"
        tolerance = diff * 0.02
        for l in self.LEVELS:
            level = swing_high - diff * l
            dist  = abs(price - level)
            if dist < tolerance:
                conf    = round(1.0 - dist / tolerance, 2)
                label   = f"{int(l*100)}%"
                fib_dir = "CALL" if l >= 0.5 else "PUT"
                return fib_dir, conf, f"Fib {label}"
        return "WAIT", 0.0, "fora de nivel fib"


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 6 — SMART MONEY CONCEPTS (v2 — com Trend obrigatório)
# ─────────────────────────────────────────────────────────────────────────────

class SmartMoneyAnalyzer:
    def _find_bos(self, candles: list) -> tuple:
        if len(candles) < 12: return None, None
        lookback   = candles[-12:-1]
        swing_high = max(c.high for c in lookback)
        swing_low  = min(c.low  for c in lookback)
        last       = candles[-1]
        if last.close > swing_high: return "BOS_UP",   swing_high
        if last.close < swing_low:  return "BOS_DOWN", swing_low
        return None, None

    def _find_order_block(self, candles: list, direction: str) -> Optional[Candle]:
        search = candles[-10:-1]
        if direction == "BOS_UP":
            for c in reversed(search):
                if c.is_bearish: return c
        elif direction == "BOS_DOWN":
            for c in reversed(search):
                if c.is_bullish: return c
        return None

    def _liquidity_sweep(self, candles: list) -> tuple:
        if len(candles) < 7: return None, 0.0
        lookback   = candles[-7:-1]
        swing_low  = min(c.low  for c in lookback)
        swing_high = max(c.high for c in lookback)
        last = candles[-1]
        if last.low < swing_low and last.close > swing_low and last.is_bullish:
            ratio = min(1.0, (swing_low - last.low) / (last.body + 1e-9) * 0.5)
            return "CALL", round(0.80 + ratio * 0.15, 2)
        if last.high > swing_high and last.close < swing_high and last.is_bearish:
            ratio = min(1.0, (last.high - swing_high) / (last.body + 1e-9) * 0.5)
            return "PUT", round(0.80 + ratio * 0.15, 2)
        return None, 0.0

    def analyze(self, candles: list) -> Signal:
        if len(candles) < 15:
            return Signal("WAIT", 0.0, "SMC: candles insuficientes")
        liq_dir, liq_conf = self._liquidity_sweep(candles)
        if liq_dir:
            return Signal(liq_dir, liq_conf, f"Liquidity Sweep {liq_dir} ({liq_conf:.2f})",
                          sr_score=liq_conf)
        bos_type, _ = self._find_bos(candles)
        if bos_type:
            ob = self._find_order_block(candles, bos_type)
            if ob:
                price  = candles[-1].close
                ob_mid = (ob.open + ob.close) / 2
                dist   = abs(price - ob_mid) / (price + 1e-9)
                if dist < 0.003:
                    d    = "CALL" if bos_type == "BOS_UP" else "PUT"
                    conf = round(max(0.65, 1.0 - dist / 0.003), 2)
                    return Signal(d, conf, f"BOS+OB {bos_type} ({conf:.2f})",
                                  sr_score=conf)
        return Signal("WAIT", 0.0, "SMC: aguardando setup")


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 7 — SIGNAL ENGINES
# ─────────────────────────────────────────────────────────────────────────────

class EnginePredicao:
    MIN_CONF = 0.72
    BULL_P   = {"Bullish Engulfing","Morning Star","Three White Soldiers",
                "Bullish Marubozu","Piercing Pattern","Hammer"}
    BEAR_P   = {"Bearish Engulfing","Evening Star","Three Black Crows",
                "Bearish Marubozu","Dark Cloud Cover","Shooting Star"}

    def __init__(self):
        self.trend = TrendAnalyzer()
        self.sr    = SupportResistance()
        self.bible = CandleBible()

    def evaluate(self, candles):
        if len(candles) < 30: return Signal("WAIT", 0.0, "candles insuficientes")
        closes = [c.close for c in candles]
        price  = closes[-1]
        trend_str, trend_conf = self.trend.analyze(closes)
        if trend_str == "SIDEWAYS": return Signal("WAIT", 0.0, "sideways")
        trend_dir = "CALL" if trend_str == "UP" else "PUT"
        if trend_conf < 0.65: return Signal("WAIT", trend_conf, f"tendencia fraca ({trend_conf:.2f})")
        candle_dir, candle_conf, pattern = self.bible.analyze(candles)
        if candle_dir == "WAIT": return Signal("WAIT", 0.0, "sem padrao candle")
        if candle_dir != trend_dir: return Signal("WAIT", 0.0, "candle contra tendencia")
        if candle_conf < 0.75: return Signal("WAIT", candle_conf, f"candle fraco ({pattern})")
        if trend_dir == "CALL" and pattern not in self.BULL_P: return Signal("WAIT", 0.0, f"{pattern} nao e touro puro")
        if trend_dir == "PUT"  and pattern not in self.BEAR_P: return Signal("WAIT", 0.0, f"{pattern} nao e urso puro")
        levels = self.sr.get_levels(candles)
        sr_dir, sr_conf = self.sr.score(price, levels)
        if sr_dir not in ("WAIT", None) and sr_dir != trend_dir:
            return Signal("WAIT", 0.0, "S/R contra tendencia")
        conf = trend_conf * 0.45 + candle_conf * 0.35 + (sr_conf * 0.20 if sr_dir == trend_dir else 0)
        if conf < self.MIN_CONF: return Signal("WAIT", conf, f"conf baixa ({conf:.2f})")
        return Signal(trend_dir, conf,
                      f"PRECISAO | {trend_str}({trend_conf:.2f}) | {pattern}({candle_conf:.2f})",
                      trend_conf, sr_conf, candle_conf)


class EngineSR:
    def __init__(self):
        self.sr    = SupportResistance()
        self.bible = CandleBible()

    def evaluate(self, candles):
        if len(candles) < 15: return Signal("WAIT", 0.0, "candles insuficientes")
        price  = candles[-1].close
        levels = self.sr.get_levels(candles)
        sr_dir, sr_conf = self.sr.score(price, levels)
        if sr_dir == "WAIT" or sr_conf < 0.50:
            return Signal("WAIT", sr_conf, f"longe de S/R ({sr_conf:.2f})")
        candle_dir, candle_conf, pattern = self.bible.analyze(candles)
        if candle_dir == sr_dir and candle_conf >= 0.65:
            conf = sr_conf * 0.55 + candle_conf * 0.45
            return Signal(sr_dir, conf,
                          f"S/R | {sr_dir}({sr_conf:.2f}) | {pattern}({candle_conf:.2f})",
                          0, sr_conf, candle_conf)
        if sr_conf >= 0.80:
            return Signal(sr_dir, sr_conf, f"S/R FORTE | {sr_dir}({sr_conf:.2f})",
                          0, sr_conf, 0)
        return Signal("WAIT", 0.0, f"S/R sem candle confirmar")


class EngineCandles:
    HIGH_CONF = {"Three White Soldiers": 0.90, "Three Black Crows": 0.90,
                 "Morning Star": 0.88, "Evening Star": 0.88,
                 "Bullish Engulfing": 0.85, "Bearish Engulfing": 0.85,
                 "Bullish Marubozu": 0.80, "Bearish Marubozu": 0.80}

    def __init__(self):
        self.bible = CandleBible()

    def evaluate(self, candles):
        if len(candles) < 10: return Signal("WAIT", 0.0, "candles insuficientes")
        candle_dir, candle_conf, pattern = self.bible.analyze(candles)
        if candle_dir == "WAIT": return Signal("WAIT", 0.0, "sem padrao")
        if pattern not in self.HIGH_CONF:
            return Signal("WAIT", 0.0, f"{pattern} — padrao fraco ignorado")
        return Signal(candle_dir, candle_conf,
                      f"CANDLE | {pattern}({candle_conf:.2f})",
                      0, 0, candle_conf)


class EngineFibonacci:
    def __init__(self):
        self.fib   = FibonacciAnalyzer()
        self.bible = CandleBible()
        self.trend = TrendAnalyzer()

    def evaluate(self, candles):
        if len(candles) < 20: return Signal("WAIT", 0.0, "candles insuficientes")
        fib_dir, fib_conf, fib_reason = self.fib.analyze(candles)
        if fib_dir == "WAIT": return Signal("WAIT", 0.0, fib_reason)
        if fib_conf < 0.55:   return Signal("WAIT", fib_conf, f"fib fraco ({fib_conf:.2f})")
        candle_dir, candle_conf, pattern = self.bible.analyze(candles)
        closes    = [c.close for c in candles]
        trend_str, trend_conf = self.trend.analyze(closes)
        trend_dir = "CALL" if trend_str == "UP" else ("PUT" if trend_str == "DOWN" else None)
        if candle_dir == fib_dir and candle_conf >= 0.65:
            conf = fib_conf * 0.50 + candle_conf * 0.35 + (trend_conf * 0.15 if trend_dir == fib_dir else 0)
            return Signal(fib_dir, conf,
                          f"FIB | {fib_reason} | {pattern}({candle_conf:.2f})",
                          trend_conf, fib_conf, candle_conf)
        if fib_conf >= 0.80:
            return Signal(fib_dir, fib_conf,
                          f"FIB FORTE | {fib_reason}", trend_conf, fib_conf, 0)
        return Signal("WAIT", 0.0, "FIB sem candle confirmar")


class EngineSmartMoney:
    """
    v2 — Trend obrigatório:
      1. SMC gera sinal (Liquidity Sweep ou BOS+OB)
      2. EMA 8/21 na MESMA direcção ← obrigatório
      3. Confiança final >= 0.70
    """
    MIN_CONF       = 0.70
    MIN_TREND_CONF = 0.30

    def __init__(self):
        self.smc   = SmartMoneyAnalyzer()
        self.trend = TrendAnalyzer()

    def evaluate(self, candles):
        if len(candles) < 25:
            return Signal("WAIT", 0.0, "candles insuficientes")

        smc_sig = self.smc.analyze(candles)
        if smc_sig.direction == "WAIT":
            return Signal("WAIT", 0.0, f"SMC: {smc_sig.reason}")

        closes    = [c.close for c in candles]
        trend_str, trend_conf = self.trend.analyze(closes)

        if trend_str == "SIDEWAYS":
            return Signal("WAIT", 0.0,
                          f"SMC BLOQUEADO: lateral (trend={trend_conf:.2f})")

        trend_dir = "CALL" if trend_str == "UP" else "PUT"

        if trend_dir != smc_sig.direction:
            return Signal("WAIT", 0.0,
                          f"SMC BLOQUEADO: {smc_sig.direction} vs Trend={trend_str}")

        if trend_conf < self.MIN_TREND_CONF:
            return Signal("WAIT", trend_conf,
                          f"SMC BLOQUEADO: trend fraca ({trend_conf:.2f})")

        conf = min(1.0, round(smc_sig.confidence * 0.65 + trend_conf * 0.35, 2))

        if conf < self.MIN_CONF:
            return Signal("WAIT", conf, f"SMC conf baixa ({conf:.2f})")

        return Signal(smc_sig.direction, conf,
                      f"SMC+TREND ✅ | {smc_sig.reason} | Trend:{trend_str}({trend_conf:.2f}) | conf:{conf:.2f}",
                      trend_conf, smc_sig.sr_score, 0)


ENGINES = {
    "🎯 Precisão Máxima":       EnginePredicao,
    "📊 Suporte & Resistência":  EngineSR,
    "🕯️ Candles Puros":         EngineCandles,
    "🌀 Fibonacci":              EngineFibonacci,
    "🧠 Smart Money (SMC) v2":  EngineSmartMoney,
}

ESTRATEGIA_INFO = {
    "🎯 Precisão Máxima":       ("Baixo",    "Triple confirm: Trend + Candle + S/R. Poucos trades, alta qualidade."),
    "📊 Suporte & Resistência":  ("Médio",    "Entra em zonas S/R com candle de reversão. Frequência média."),
    "🕯️ Candles Puros":         ("Médio",    "Só padrões premium: 3 Soldiers, Engulfing, Stars."),
    "🌀 Fibonacci":              ("Médio",    "Retracções 38.2%, 50%, 61.8% com confirmação."),
    "🧠 Smart Money (SMC) v2":  ("Alto ⚠️",  "Order Blocks + BOS + Liquidity Sweep + Trend obrigatório. Só demo!"),
}


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 8 — SESSION MANAGER  (+ stop por perdas consecutivas)
# ─────────────────────────────────────────────────────────────────────────────

class SessionManager:
    def __init__(self):
        self._lock           = threading.Lock()
        self._trades         = []
        self._logs           = deque(maxlen=200)
        self._signals        = deque(maxlen=40)
        self._pnl            = 0.0
        self._wins           = 0
        self._losses         = 0
        self._consec_losses  = 0   # perdas consecutivas actuais
        self._max_consec     = 0   # recorde de perdas consecutivas

    def add_trade(self, symbol, direction, stake, profit, signal_reason=""):
        with self._lock:
            entry = {"time": datetime.now().strftime("%H:%M:%S"),
                     "symbol": symbol, "direction": direction,
                     "stake": stake, "profit": profit, "signal": signal_reason}
            self._trades.append(entry)
            self._pnl += profit
            if profit > 0:
                self._wins          += 1
                self._consec_losses  = 0
            else:
                self._losses        += 1
                self._consec_losses += 1
                self._max_consec     = max(self._max_consec, self._consec_losses)
            r = f"✅ +${profit:.2f}" if profit > 0 else f"❌ ${profit:.2f}"
            self._logs.append(f"[{entry['time']}] {direction} {symbol} {r}")

    def add_signal(self, direction, reason):
        with self._lock:
            self._signals.append({"time": datetime.now().strftime("%H:%M:%S"),
                                  "dir": direction, "reason": reason})

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
            return {"pnl": round(self._pnl, 2), "trades": total,
                    "wins": self._wins, "losses": self._losses,
                    "winrate": winrate,
                    "consec_losses": self._consec_losses,
                    "max_consec": self._max_consec}

    def pnl(self):
        with self._lock: return self._pnl

    def reset(self):
        with self._lock:
            self._trades = []; self._pnl = 0.0
            self._wins = 0; self._losses = 0
            self._consec_losses = 0; self._max_consec = 0
            self._signals.clear(); self._logs.clear()


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 9 — DERIV CLIENT (PAT → REST → OTP → WebSocket)
# ─────────────────────────────────────────────────────────────────────────────

DERIV_REST_BASE = "https://api.derivws.com"

class DerivClient:
    def __init__(self, pat_token: str, app_id: str, account_type: str = "demo"):
        self.pat            = pat_token
        self.app_id         = app_id
        self.account_type   = account_type
        self._ws            = None
        self._req_id        = 1
        self._pending       = {}
        self._candles_q     = asyncio.Queue(maxsize=1000)
        self._listener_task = None
        self._account_id    = None

    def _headers(self):
        return {"Authorization": f"Bearer {self.pat}",
                "Deriv-App-ID":  self.app_id,
                "Content-Type":  "application/json"}

    async def _get_account_id(self) -> str:
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=self._headers()) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise PermissionError(f"Erro contas: {body}")
                for acc in body.get("data", []):
                    if acc.get("account_type") == self.account_type and acc.get("status") == "active":
                        return acc["account_id"]
                if self.account_type == "demo":
                    return await self._create_demo_account()
                raise RuntimeError(f"Nenhuma conta '{self.account_type}' ativa.")

    async def _create_demo_account(self) -> str:
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=self._headers(),
                              json={"currency": "USD", "group": "row",
                                    "account_type": "demo"}) as resp:
                body = await resp.json()
                if resp.status not in (200, 201):
                    raise RuntimeError(f"Erro criar demo: {body}")
                return body["data"]["account_id"]

    async def _get_ws_url(self, account_id: str) -> str:
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts/{account_id}/otp"
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=self._headers()) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise PermissionError(f"Erro OTP: {body}")
                ws_url = body.get("data", {}).get("url")
                if not ws_url:
                    raise RuntimeError(f"URL WS nao encontrado: {body}")
                return ws_url

    async def connect(self, retries: int = 3):
        last_err = None
        for attempt in range(1, retries + 1):
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
                if attempt < retries: await asyncio.sleep(3 * attempt)
        raise ConnectionError(f"Falha apos {retries} tentativas: {last_err}")

    async def disconnect(self):
        if self._listener_task: self._listener_task.cancel()
        if self._ws: await self._ws.close()

    async def _send(self, payload: dict, timeout: float = 15.0) -> dict:
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

    async def subscribe_candles(self, symbol: str, granularity: int = 60):
        resp = await self._send({"ticks_history": symbol, "style": "candles",
                                 "granularity": granularity, "count": 60,
                                 "end": "latest", "subscribe": 1})
        if resp.get("error"): raise RuntimeError(resp["error"]["message"])
        return resp.get("candles", [])

    async def get_candle_update(self, timeout: float = 90.0) -> dict:
        return await asyncio.wait_for(self._candles_q.get(), timeout)

    async def buy_contract(self, symbol, direction, stake, duration, duration_unit="t") -> dict:
        proposal = await self._send({
            "proposal": 1, "amount": stake, "basis": "stake",
            "contract_type": direction, "currency": "USD",
            "duration": duration, "duration_unit": duration_unit,
            "underlying_symbol": symbol})
        if proposal.get("error"): raise RuntimeError(proposal["error"]["message"])
        buy = await self._send({"buy": proposal["proposal"]["id"], "price": stake})
        if buy.get("error"): raise RuntimeError(buy["error"]["message"])
        return buy["buy"]

    async def get_contract_result(self, contract_id: int, max_wait: float = 120.0) -> dict:
        deadline = asyncio.get_event_loop().time() + max_wait
        while asyncio.get_event_loop().time() < deadline:
            resp = await self._send({"proposal_open_contract": 1,
                                     "contract_id": contract_id})
            poc  = resp.get("proposal_open_contract", {})
            if poc.get("is_sold") or poc.get("status") in ("sold", "won", "lost"):
                return {"profit": float(poc.get("profit", 0)),
                        "status": poc.get("status")}
            await asyncio.sleep(2)
        raise TimeoutError("Contrato nao liquidou a tempo")

    async def get_balance(self) -> float:
        url = f"{DERIV_REST_BASE}/trading/v1/options/accounts"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=self._headers()) as resp:
                body = await resp.json()
                for acc in body.get("data", []):
                    if acc.get("account_id") == self._account_id:
                        return float(acc.get("balance", 0))
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 10 — BOT ORCHESTRATOR  (stop por perdas consecutivas)
# ─────────────────────────────────────────────────────────────────────────────

_DURATION_MAP = {
    "1 tique": (1,"t"), "5 tiques": (5,"t"), "10 tiques": (10,"t"),
    "15s": (15,"s"), "30s": (30,"s"), "1m": (1,"m"), "5m": (5,"m"),
}
_GRANULARITY = {"t": 60, "s": 60, "m": 300}


class DerivBot:
    COOLDOWN_SECS   = 60
    MAX_TRADES_HOUR = 12

    def __init__(self, config: dict, manager: SessionManager):
        self.cfg            = config
        self.manager        = manager
        self._stop          = False
        dur                 = config.get("duration", "5 tiques")
        self.dur_val, self.dur_unit = _DURATION_MAP.get(dur, (5, "t"))
        self.granularity    = _GRANULARITY.get(self.dur_unit, 60)
        engine_class        = ENGINES.get(config.get("estrategia"), EnginePredicao)
        self.engine         = engine_class()
        self.client         = DerivClient(config["api_token"], config["app_id"],
                                          config.get("account_type", "demo"))
        self.base_stake     = float(config.get("stake", 1.0))
        self.current_stake  = self.base_stake
        self.ml_mult        = float(config.get("mult", 2.0))
        self.ml_enabled     = bool(config.get("martingale", False))
        self.max_consec     = int(config.get("max_consec_losses", 3))
        self.candles        = []

    def stop(self): self._stop = True

    async def run(self):
        symbol = self.cfg["symbol"]
        strat  = self.cfg.get("estrategia", "?")
        self.manager.log(f"🚀 Bot iniciando | {strat}")
        self.manager.log(f"📌 {symbol} | {self.cfg.get('account_type','demo').upper()} "
                         f"| stake=${self.base_stake} | ML={'ON' if self.ml_enabled else 'OFF'} "
                         f"| max_consec={self.max_consec}")

        last_trade_time  = 0.0
        trades_this_hour = []

        try:
            await self.client.connect()
            balance = await self.client.get_balance()
            self.manager.log(f"✅ Conectado | {self.client._account_id} | saldo=${balance:.2f}")

            raw = await self.client.subscribe_candles(symbol, self.granularity)
            for r in raw:
                self.candles.append(Candle(float(r["open"]), float(r["high"]),
                                           float(r["low"]),  float(r["close"]),
                                           int(r.get("epoch", 0))))
            self.candles = self.candles[-100:]
            self.manager.log(f"📊 {len(self.candles)} candles históricos carregados")

            while not self._stop:
                stats = self.manager.stats()

                # ── Limites de encerramento ────────────────────────────────────
                if stats["pnl"] >= self.cfg["daily_goal"]:
                    self.manager.log(f"🎯 Meta diária atingida! (${stats['pnl']:.2f})"); break
                if stats["pnl"] <= -self.cfg["max_loss"]:
                    self.manager.log(f"🛑 Stop loss activado! (${stats['pnl']:.2f})"); break
                if stats["consec_losses"] >= self.max_consec:
                    self.manager.log(
                        f"🛑 Stop por {stats['consec_losses']} perdas consecutivas! "
                        f"Aguarda 30 min antes de reiniciar."); break

                # ── Novo candle ────────────────────────────────────────────────
                try:
                    msg  = await self.client.get_candle_update(timeout=90)
                    ohlc = msg.get("ohlc", {})
                    if ohlc:
                        c = Candle(float(ohlc["open"]), float(ohlc["high"]),
                                   float(ohlc["low"]),  float(ohlc["close"]),
                                   int(ohlc.get("epoch", 0)))
                        if not self.candles or c.epoch != self.candles[-1].epoch:
                            self.candles.append(c)
                            if len(self.candles) > 100:
                                self.candles = self.candles[-100:]
                except asyncio.TimeoutError:
                    self.manager.log("⏳ Sem candle 90s — aguardando..."); continue

                if len(self.candles) < 25: continue

                # ── Avaliar sinal ──────────────────────────────────────────────
                signal = self.engine.evaluate(self.candles)
                self.manager.add_signal(signal.direction, signal.reason)
                if signal.direction == "WAIT": continue

                # ── Cooldown ───────────────────────────────────────────────────
                now     = time.time()
                elapsed = now - last_trade_time
                if elapsed < self.COOLDOWN_SECS:
                    self.manager.add_signal("WAIT",
                        f"cooldown: {int(self.COOLDOWN_SECS - elapsed)}s"); continue

                # ── Limite horário ─────────────────────────────────────────────
                trades_this_hour = [t for t in trades_this_hour if now - t < 3600]
                if len(trades_this_hour) >= self.MAX_TRADES_HOUR:
                    self.manager.add_signal("WAIT", f"limite {self.MAX_TRADES_HOUR}/hora")
                    await asyncio.sleep(30); continue

                # ── Log do sinal ───────────────────────────────────────────────
                self.manager.log(
                    f"📡 {signal.direction} | conf={signal.confidence:.2f} | "
                    f"consec_loss={stats['consec_losses']}")
                self.manager.log(f"   {signal.reason[:90]}")

                # ── Executar trade ─────────────────────────────────────────────
                try:
                    buy_info    = await self.client.buy_contract(
                        symbol, signal.direction,
                        self.current_stake, self.dur_val, self.dur_unit)
                    contract_id = buy_info.get("contract_id")
                    self.manager.log(
                        f"📝 ID:{contract_id} | stake=${self.current_stake:.2f}")

                    result = await self.client.get_contract_result(contract_id)
                    profit = result["profit"]
                    self.manager.add_trade(symbol, signal.direction,
                                           self.current_stake, profit,
                                           signal.reason[:80])

                    last_trade_time = time.time()
                    trades_this_hour.append(last_trade_time)

                    if profit > 0:
                        self.current_stake = self.base_stake
                        self.manager.log(f"✅ WIN +${profit:.2f} | stake reset ${self.base_stake:.2f}")
                    else:
                        cl = self.manager.consec_losses()
                        self.manager.log(f"❌ LOSS ${profit:.2f} | consec={cl}/{self.max_consec}")
                        if self.ml_enabled:
                            self.current_stake = min(
                                round(self.current_stake * self.ml_mult, 2),
                                self.base_stake * 8)
                            self.manager.log(f"📈 Martingale: ${self.current_stake:.2f}")
                        else:
                            self.current_stake = self.base_stake

                    await asyncio.sleep(2)

                except Exception as e:
                    self.manager.log(f"❌ Erro trade: {e}"); await asyncio.sleep(5)

        except Exception as e:
            self.manager.log(f"💥 Erro crítico: {e}")
        finally:
            await self.client.disconnect()
            self.manager.log("🔌 Desconectado. Bot encerrado.")


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 11 — STREAMLIT DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Deriv Bot Pro v2", page_icon="⚡",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;700&display=swap');
html,body,[class*="css"]{ font-family:'Space Grotesk',sans-serif; }
.stApp{ background:#0a0e1a; color:#e0e6f5; }
.metric-card{ background:#111827; border:1px solid #1e3a5f; border-radius:12px; padding:18px; text-align:center; }
.profit { color:#00d4aa; font-family:'JetBrains Mono',monospace; font-size:1.8rem; font-weight:700; }
.loss   { color:#ff4d6d; font-family:'JetBrains Mono',monospace; font-size:1.8rem; font-weight:700; }
.neutral{ color:#7c9cbf; font-family:'JetBrains Mono',monospace; font-size:1.8rem; font-weight:700; }
.warn   { color:#f59e0b; font-family:'JetBrains Mono',monospace; font-size:1.8rem; font-weight:700; }
.strat-card{ background:#111827; border:1px solid #1e3a5f; border-radius:10px; padding:12px 16px; margin:8px 0; }
.signal-box { background:#111827; border-left:4px solid #00d4aa; border-radius:8px; padding:10px 14px; margin:5px 0; font-family:'JetBrains Mono',monospace; font-size:.82rem; }
.signal-sell{ border-left-color:#ff4d6d; }
.signal-wait{ border-left-color:#f59e0b; }
.dot-green  { width:10px;height:10px;background:#00d4aa;border-radius:50%;display:inline-block;margin-right:6px; }
.dot-red    { width:10px;height:10px;background:#ff4d6d;border-radius:50%;display:inline-block;margin-right:6px; }
.dot-yellow { width:10px;height:10px;background:#f59e0b;border-radius:50%;display:inline-block;margin-right:6px; }
.stButton>button{ background:linear-gradient(135deg,#00d4aa,#0099ff); color:#0a0e1a; font-weight:700; border:none; border-radius:8px; }
.risk-low { color:#00d4aa; font-weight:700; }
.risk-med { color:#f59e0b; font-weight:700; }
.risk-high{ color:#ff4d6d; font-weight:700; }
</style>
""", unsafe_allow_html=True)

for k, v in [("bot", None), ("running", False), ("manager", SessionManager())]:
    if k not in st.session_state: st.session_state[k] = v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Deriv Bot Pro v2")
    st.markdown("---")

    api_key = st.text_input("🔑 PAT Token", type="password",
                             value=os.environ.get("DERIV_API_TOKEN", ""))
    app_id  = st.text_input("🆔 App ID",
                             value=os.environ.get("DERIV_APP_ID", ""))

    st.markdown("---")
    st.markdown("### 🎮 Estratégia")
    estrategia = st.selectbox("Modo de operação", list(ENGINES.keys()))
    risco, descricao = ESTRATEGIA_INFO[estrategia]
    rc = "risk-high" if "Alto" in risco else ("risk-med" if "Médio" in risco else "risk-low")
    st.markdown(f"""
    <div class="strat-card">
        <div style="font-size:.75rem;color:#7c9cbf">RISCO</div>
        <div class="{rc}">{risco}</div>
        <div style="font-size:.8rem;color:#a0b0c8;margin-top:6px">{descricao}</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("---")
    account_type = st.selectbox("Conta", ["demo", "real"])
    symbol       = st.selectbox("Ativo", ["R_100","R_75","R_50","R_25","R_10",
                                           "1HZ100V","1HZ75V","frxEURUSD","frxGBPUSD","frxUSDJPY"])
    duration     = st.selectbox("Duração", ["1 tique","5 tiques","10 tiques","15s","30s","1m","5m"])
    stake        = st.number_input("Aposta (USD)", min_value=0.35, max_value=100.0, value=1.0, step=0.5)
    daily_goal   = st.number_input("Meta Diária (USD)", value=5.0, step=0.5)
    max_loss     = st.number_input("Stop Loss (USD)", value=2.0, step=0.5)

    st.markdown("---")
    st.markdown("### 🛡️ Gestão de Risco")
    max_consec = st.number_input("Stop p/ perdas consecutivas", min_value=1, max_value=10, value=3, step=1,
                                  help="Bot para automaticamente após N perdas seguidas")
    martingale = st.toggle("Martingale", value=False)
    mult       = st.slider("Multiplicador", 1.5, 3.0, 2.0, 0.5) if martingale else 1.0

    st.markdown("---")
    c1, c2 = st.columns(2)
    start_btn = c1.button("▶ Iniciar", use_container_width=True)
    stop_btn  = c2.button("⏹ Parar",  use_container_width=True)

# ── Header ────────────────────────────────────────────────────────────────────
col_t, col_s = st.columns([3, 1])
with col_t:
    st.markdown("# ⚡ Deriv Bot Pro v2")
    st.markdown(f"*Estratégia: **{estrategia}***")
with col_s:
    if st.session_state.running:
        st.markdown('<div style="padding:12px 0"><span class="dot-green"></span><b>ONLINE</b></div>',
                    unsafe_allow_html=True)
    else:
        st.markdown('<div style="padding:12px 0"><span class="dot-red"></span><b>OFFLINE</b></div>',
                    unsafe_allow_html=True)

# ── Métricas (6 cards) ────────────────────────────────────────────────────────
manager = st.session_state.manager
stats   = manager.stats()

m1, m2, m3, m4, m5, m6 = st.columns(6)
with m1:
    cls = "profit" if stats["pnl"] >= 0 else "loss"
    st.markdown(f'<div class="metric-card"><div style="font-size:.75rem;color:#7c9cbf">P&L HOJE</div>'
                f'<div class="{cls}">${stats["pnl"]:.2f}</div></div>', unsafe_allow_html=True)
with m2:
    st.markdown(f'<div class="metric-card"><div style="font-size:.75rem;color:#7c9cbf">TRADES</div>'
                f'<div class="neutral">{stats["trades"]}</div></div>', unsafe_allow_html=True)
with m3:
    wc = "profit" if stats["winrate"] >= 60 else ("neutral" if stats["winrate"] >= 50 else "loss")
    st.markdown(f'<div class="metric-card"><div style="font-size:.75rem;color:#7c9cbf">WIN RATE</div>'
                f'<div class="{wc}">{stats["winrate"]:.1f}%</div></div>', unsafe_allow_html=True)
with m4:
    gp = min(100, stats["pnl"] / daily_goal * 100) if stats["pnl"] > 0 and daily_goal > 0 else 0
    st.markdown(f'<div class="metric-card"><div style="font-size:.75rem;color:#7c9cbf">META ({gp:.0f}%)</div>'
                f'<div class="profit">${daily_goal:.2f}</div></div>', unsafe_allow_html=True)
with m5:
    lv = abs(min(0, stats["pnl"]))
    lc = "loss" if max_loss > 0 and lv/max_loss > 0.7 else ("neutral" if max_loss > 0 and lv/max_loss > 0.4 else "profit")
    st.markdown(f'<div class="metric-card"><div style="font-size:.75rem;color:#7c9cbf">STOP LOSS</div>'
                f'<div class="{lc}">${max_loss:.2f}</div></div>', unsafe_allow_html=True)
with m6:
    cl    = stats["consec_losses"]
    cl_c  = "loss" if cl >= max_consec else ("warn" if cl >= max_consec - 1 else "neutral")
    st.markdown(f'<div class="metric-card"><div style="font-size:.75rem;color:#7c9cbf">CONS. LOSS</div>'
                f'<div class="{cl_c}">{cl}/{max_consec}</div></div>', unsafe_allow_html=True)

st.markdown("")

# ── Layout principal ──────────────────────────────────────────────────────────
left, right = st.columns([2, 1])

with left:
    st.markdown("### 📊 Histórico de Trades")
    trades = manager.get_trades()
    if trades:
        df = pd.DataFrame(trades)
        df["resultado"] = df["profit"].apply(
            lambda x: f"✅ +${x:.2f}" if x > 0 else f"❌ ${x:.2f}")
        cols = [c for c in ["time","symbol","direction","stake","resultado","signal"]
                if c in df.columns]
        st.dataframe(df[cols].tail(20), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum trade ainda. Inicia o bot para começar.")

    cp, cl_col = st.columns(2)
    with cp:
        pp = min(1.0, max(0, stats["pnl"]) / daily_goal) if daily_goal > 0 else 0
        st.markdown(f"**Meta: ${max(0, stats['pnl']):.2f} / ${daily_goal:.2f}**")
        st.progress(pp)
    with cl_col:
        lp = min(1.0, abs(min(0, stats["pnl"])) / max_loss) if max_loss > 0 else 0
        st.markdown(f"**Stop: ${abs(min(0, stats['pnl'])):.2f} / ${max_loss:.2f}**")
        st.progress(lp)

with right:
    st.markdown("### 🔍 Sinais ao Vivo")
    signals = manager.get_signals()
    if signals:
        for s in signals[-8:]:
            bc  = ("signal-box" if s["dir"] == "CALL"
                   else "signal-box signal-sell" if s["dir"] == "PUT"
                   else "signal-box signal-wait")
            ico = "🟢" if s["dir"] == "CALL" else ("🔴" if s["dir"] == "PUT" else "🟡")
            st.markdown(
                f'<div class="{bc}">{ico} <b>{s["dir"]}</b> &nbsp; {s["time"]}<br>'
                f'<span style="color:#7c9cbf">{s["reason"]}</span></div>',
                unsafe_allow_html=True)
    else:
        st.info("Aguardando sinais…")

    st.markdown("### 📋 Log")
    logs = manager.get_logs()
    html = ""
    for e in logs[-14:]:
        cor = ("#00d4aa" if "✅" in e
               else "#ff4d6d" if "❌" in e or "💥" in e or "🛑" in e
               else "#f59e0b" if "⏳" in e or "BLOQUEADO" in e
               else "#7c9cbf")
        html += (f'<div style="font-family:JetBrains Mono,monospace;font-size:.74rem;'
                 f'color:{cor};padding:2px 0">{e}</div>')
    st.markdown(f'<div style="background:#111827;border-radius:8px;padding:12px;'
                f'max-height:300px;overflow-y:auto">{html}</div>', unsafe_allow_html=True)

# ── Start / Stop ──────────────────────────────────────────────────────────────
if start_btn and not st.session_state.running:
    if not api_key:
        st.error("❌ Insere o PAT Token!")
    elif not app_id:
        st.error("❌ Insere o App ID!")
    else:
        cfg = {"api_token": api_key, "app_id": app_id,
               "account_type": account_type, "estrategia": estrategia,
               "symbol": symbol, "duration": duration, "stake": stake,
               "daily_goal": daily_goal, "max_loss": max_loss,
               "martingale": martingale, "mult": mult,
               "max_consec_losses": max_consec}
        st.session_state.manager.reset()
        bot = DerivBot(cfg, st.session_state.manager)
        st.session_state.bot     = bot
        st.session_state.running = True
        threading.Thread(target=lambda: asyncio.run(bot.run()), daemon=True).start()
        st.success(f"✅ Bot iniciado com estratégia: {estrategia}")
        time.sleep(1); st.rerun()

if stop_btn and st.session_state.running:
    if st.session_state.bot: st.session_state.bot.stop()
    st.session_state.running = False
    st.warning("⏹ Bot parado.")
    time.sleep(1); st.rerun()

if st.session_state.running:
    time.sleep(3); st.rerun()
