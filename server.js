'use strict';

const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({
  server,
  path: '/ws'
});

const PORT = process.env.PORT || 10000;

// ============================================================
// SECURITY / CONFIG
// ============================================================

const BOT_PASSWORD =
  process.env.BOT_PASSWORD || 'change-this-password';

const MT5_BRIDGE_SECRET =
  process.env.MT5_BRIDGE_SECRET || 'change-this-secret';

// AI API KEYS
const OPENAI_API_KEY =
  process.env.OPENAI_API_KEY || '';

const DEEPSEEK_API_KEY =
  process.env.DEEPSEEK_API_KEY || '';

const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY || '';

const GROQ_API_KEY =
  process.env.GROQ_API_KEY || '';

const OPENROUTER_API_KEY =
  process.env.OPENROUTER_API_KEY || '';

const MISTRAL_API_KEY =
  process.env.MISTRAL_API_KEY || '';

// ============================================================
// AI MODELS
// ============================================================

const AI_MODELS = {
  openai:
    process.env.OPENAI_MODEL || 'gpt-5.6-luna',

  deepseek:
    process.env.DEEPSEEK_MODEL || 'deepseek-v4-flash',

  gemini:
    process.env.GEMINI_MODEL || 'gemini-2.5-flash',

  groq:
    process.env.GROQ_MODEL || 'openai/gpt-oss-20b',

  openrouter:
    process.env.OPENROUTER_MODEL || 'openrouter/free',

  mistral:
    process.env.MISTRAL_MODEL || 'mistral-large-latest'
};

// ============================================================
// EXPRESS
// ============================================================

app.use(express.json({
  limit: '2mb'
}));

app.use(
  express.static(
    path.join(__dirname, 'public')
  )
);

// ============================================================
// GLOBAL STATE
// ============================================================

const state = {

  connected: false,

  symbol: 'XAUUSD',

  timeframe: 'M5',

  mode: 'ANALYSIS ONLY',

  price: null,

  bid: null,

  ask: null,

  spread: null,

  tickTime: null,

  candles: [],

  technical: {

    rsi: null,

    ema20: null,

    ema50: null,

    ema200: null,

    atr: null,

    momentum: null,

    trend: 'WAITING',

    support: null,

    resistance: null,

    candleStructure: 'WAITING'
  },

  news: {

    event: 'NO LIVE NEWS DATA',

    currency: 'USD',

    impact: 'UNKNOWN',

    actual: null,

    forecast: null,

    previous: null,

    releaseTime: null,

    status: 'WAITING'
  },

  ai: {

    running: false,

    lastRun: null,

    analysts: {

      openai: emptyAnalyst('OpenAI'),

      deepseek: emptyAnalyst('DeepSeek'),

      gemini: emptyAnalyst('Gemini'),

      groq: emptyAnalyst('Groq'),

      openrouter: emptyAnalyst('OpenRouter'),

      mistral: emptyAnalyst('Mistral AI')
    },

    consensus: {

      action: 'WAIT',

      confidence: 0,

      agreement: 0,

      buy: 0,

      sell: 0,

      wait: 100,

      activeAnalysts: 0,

      totalAnalysts: 6,

      reason:
        'Waiting for AI analysis.'
    }
  },

  forecast: {

    direction: 'WAIT',

    confidence: 0,

    reason:
      'Waiting for AI analysis.'
  },

  decision: {

    action: 'WAIT',

    confidence: 0,

    agreement: 0,

    entry: null,

    stopLoss: null,

    takeProfit: null,

    reason:
      'Waiting for live market data.'
  },

  safety: {

    safe: false,

    spreadOK: false,

    staleData: true,

    newsRisk: false,

    riskOK: true,

    message:
      'Waiting for MT5 connection.'
  },

  positions: [],

  lastUpdate: null,

  clients: new Set()
};

// ============================================================
// HELPERS
// ============================================================

function emptyAnalyst(name) {

  return {

    provider: name,

    status: 'WAITING',

    action: 'WAIT',

    confidence: 0,

    reason:
      'AI analysis has not started.',

    model: null,

    latency: null,

    error: null,

    timestamp: null
  };
}

function safeNumber(value) {

  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : null;
}

function round(value, digits = 2) {

  if (!Number.isFinite(Number(value))) {
    return null;
  }

  return Number(
    Number(value).toFixed(digits)
  );
}

function clamp(value, min, max) {

  return Math.max(
    min,
    Math.min(max, value)
  );
}

function nowISO() {

  return new Date().toISOString();
}

function constantTimeEqual(a, b) {

  const aa =
    Buffer.from(String(a || ''));

  const bb =
    Buffer.from(String(b || ''));

  if (aa.length !== bb.length) {
    return false;
  }

  return crypto.timingSafeEqual(
    aa,
    bb
  );
}

function send(ws, payload) {

  if (
    ws &&
    ws.readyState === WebSocket.OPEN
  ) {

    ws.send(
      JSON.stringify(payload)
    );
  }
}

function broadcast(payload) {

  const message =
    JSON.stringify(payload);

  for (
    const ws of state.clients
  ) {

    if (
      ws.readyState ===
      WebSocket.OPEN
    ) {

      ws.send(message);
    }
  }
}

// ============================================================
// PUBLIC STATE
// ============================================================

function publicState() {

  return {

    connected:
      state.connected,

    symbol:
      state.symbol,

    timeframe:
      state.timeframe,

    mode:
      state.mode,

    price:
      state.price,

    bid:
      state.bid,

    ask:
      state.ask,

    spread:
      state.spread,

    tickTime:
      state.tickTime,

    candles:
      state.candles.slice(-500),

    technical:
      state.technical,

    news:
      state.news,

    ai:
      state.ai,

    forecast:
      state.forecast,

    decision:
      state.decision,

    safety:
      state.safety,

    positions:
      state.positions,

    lastUpdate:
      state.lastUpdate
  };
}

// ============================================================
// TECHNICAL ENGINE
// ============================================================

function calculateEMA(values, period) {

  if (
    !Array.isArray(values) ||
    values.length < period
  ) {

    return null;
  }

  const multiplier =
    2 / (period + 1);

  let ema =
    values
      .slice(0, period)
      .reduce(
        (a, b) => a + b,
        0
      ) / period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {

    ema =
      ((values[i] - ema) *
        multiplier) +
      ema;
  }

  return ema;
}

function calculateRSI(
  values,
  period = 14
) {

  if (
    !Array.isArray(values) ||
    values.length <= period
  ) {

    return null;
  }

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {

    const difference =
      values[i] -
      values[i - 1];

    if (difference >= 0) {

      gains += difference;

    } else {

      losses +=
        Math.abs(difference);
    }
  }

  let averageGain =
    gains / period;

  let averageLoss =
    losses / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {

    const difference =
      values[i] -
      values[i - 1];

    const gain =
      Math.max(
        difference,
        0
      );

    const loss =
      Math.max(
        -difference,
        0
      );

    averageGain =
      (
        averageGain *
        (period - 1) +
        gain
      ) / period;

    averageLoss =
      (
        averageLoss *
        (period - 1) +
        loss
      ) / period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  const rs =
    averageGain /
    averageLoss;

  return (
    100 -
    100 / (1 + rs)
  );
}

function calculateATR(
  candles,
  period = 14
) {

  if (
    !Array.isArray(candles) ||
    candles.length <= period
  ) {

    return null;
  }

  const ranges = [];

  for (
    let i = 1;
    i < candles.length;
    i++
  ) {

    const current =
      candles[i];

    const previous =
      candles[i - 1];

    const high =
      safeNumber(current.high);

    const low =
      safeNumber(current.low);

    const previousClose =
      safeNumber(previous.close);

    if (
      high === null ||
      low === null ||
      previousClose === null
    ) {

      continue;
    }

    const tr =
      Math.max(
        high - low,

        Math.abs(
          high -
          previousClose
        ),

        Math.abs(
          low -
          previousClose
        )
      );

    ranges.push(tr);
  }

  if (
    ranges.length < period
  ) {

    return null;
  }

  let atr =
    ranges
      .slice(0, period)
      .reduce(
        (a, b) => a + b,
        0
      ) / period;

  for (
    let i = period;
    i < ranges.length;
    i++
  ) {

    atr =
      (
        atr * (period - 1) +
        ranges[i]
      ) / period;
  }

  return atr;
}

function calculateTechnicalAnalysis() {

  const candles =
    state.candles;

  if (
    candles.length < 20
  ) {

    return;
  }

  const closes =
    candles
      .map(c =>
        safeNumber(c.close)
      )
      .filter(
        v => v !== null
      );

  if (
    closes.length < 20
  ) {

    return;
  }

  const ema20 =
    calculateEMA(
      closes,
      20
    );

  const ema50 =
    calculateEMA(
      closes,
      50
    );

  const ema200 =
    calculateEMA(
      closes,
      200
    );

  const rsi =
    calculateRSI(
      closes,
      14
    );

  const atr =
    calculateATR(
      candles,
      14
    );

  const last =
    closes[closes.length - 1];

  const previous =
    closes[closes.length - 2];

  let momentum = null;

  if (previous !== 0) {

    momentum =
      (
        (last - previous) /
        previous
      ) * 100;
  }

  let trend =
    'NEUTRAL';

  if (
    ema20 !== null &&
    ema50 !== null
  ) {

    if (
      last > ema20 &&
      ema20 > ema50
    ) {

      trend =
        'BULLISH';

    } else if (
      last < ema20 &&
      ema20 < ema50
    ) {

      trend =
        'BEARISH';
    }
  }

  const recent =
    candles.slice(-50);

  const highs =
    recent
      .map(c =>
        safeNumber(c.high)
      )
      .filter(
        v => v !== null
      );

  const lows =
    recent
      .map(c =>
        safeNumber(c.low)
      )
      .filter(
        v => v !== null
      );

  const resistance =
    highs.length
      ? Math.max(...highs)
      : null;

  const support =
    lows.length
      ? Math.min(...lows)
      : null;

  const lastCandle =
    candles[candles.length - 1];

  let candleStructure =
    'NEUTRAL';

  if (lastCandle) {

    const open =
      safeNumber(
        lastCandle.open
      );

    const high =
      safeNumber(
        lastCandle.high
      );

    const low =
      safeNumber(
        lastCandle.low
      );

    const close =
      safeNumber(
        lastCandle.close
      );

    if (
      open !== null &&
      high !== null &&
      low !== null &&
      close !== null
    ) {

      const body =
        Math.abs(
          close - open
        );

      const range =
        high - low;

      if (range > 0) {

        if (
          body / range >
          0.65
        ) {

          candleStructure =
            close > open
              ? 'STRONG BULLISH'
              : 'STRONG BEARISH';

        } else {

          candleStructure =
            'MIXED';
        }
      }
    }
  }

  state.technical = {

    rsi:
      round(rsi, 2),

    ema20:
      round(ema20, 5),

    ema50:
      round(ema50, 5),

    ema200:
      round(ema200, 5),

    atr:
      round(atr, 5),

    momentum:
      round(momentum, 4),

    trend,

    support:
      round(support, 5),

    resistance:
      round(resistance, 5),

    candleStructure
  };
}

// ============================================================
// TECHNICAL DECISION
// ============================================================

function runTechnicalDecision() {

  if (
    state.price === null ||
    state.candles.length < 20
  ) {

    return;
  }

  const t =
    state.technical;

  let buy = 0;
  let sell = 0;

  if (t.rsi !== null) {

    if (
      t.rsi >= 55 &&
      t.rsi <= 70
    ) {

      buy += 20;
    }

    if (
      t.rsi <= 45 &&
      t.rsi >= 30
    ) {

      sell += 20;
    }
  }

  if (
    t.ema20 !== null &&
    t.ema50 !== null
  ) {

    if (
      state.price > t.ema20 &&
      t.ema20 > t.ema50
    ) {

      buy += 25;
    }

    if (
      state.price < t.ema20 &&
      t.ema20 < t.ema50
    ) {

      sell += 25;
    }
  }

  if (t.momentum !== null) {

    if (t.momentum > 0) {
      buy += 15;
    }

    if (t.momentum < 0) {
      sell += 15;
    }
  }

  if (
    t.candleStructure ===
    'STRONG BULLISH'
  ) {

    buy += 20;
  }

  if (
    t.candleStructure ===
    'STRONG BEARISH'
  ) {

    sell += 20;
  }

  if (t.trend === 'BULLISH') {
    buy += 20;
  }

  if (t.trend === 'BEARISH') {
    sell += 20;
  }

  const total =
    buy + sell;

  if (total <= 0) {

    state.decision = {

      ...state.decision,

      action: 'WAIT',

      confidence: 0,

      reason:
        'Technical signals are insufficient.'
    };

    return;
  }

  const buyPct =
    Math.round(
      buy / total * 100
    );

  const sellPct =
    Math.round(
      sell / total * 100
    );

  let action =
    'WAIT';

  if (
    Math.max(
      buyPct,
      sellPct
    ) >= 65
  ) {

    action =
      buyPct > sellPct
        ? 'BUY'
        : 'SELL';
  }

  let stopLoss = null;
  let takeProfit = null;

  if (
    t.atr !== null &&
    state.price !== null
  ) {

    if (action === 'BUY') {

      stopLoss =
        state.price -
        t.atr * 1.5;

      takeProfit =
        state.price +
        t.atr * 3;

    } else if (
      action === 'SELL'
    ) {

      stopLoss =
        state.price +
        t.atr * 1.5;

      takeProfit =
        state.price -
        t.atr * 3;
    }
  }

  state.decision = {

    ...state.decision,

    action,

    confidence:
      Math.max(
        buyPct,
        sellPct
      ),

    entry:
      round(
        state.price,
        5
      ),

    stopLoss:
      round(
        stopLoss,
        5
      ),

    takeProfit:
      round(
        takeProfit,
        5
      ),

    reason:
      action === 'BUY'
        ? 'Technical conditions favor BUY.'
        : action === 'SELL'
          ? 'Technical conditions favor SELL.'
          : 'Technical signals are not sufficiently aligned.'
  };
}

// ============================================================
// AI PROMPT
// ============================================================

function buildAIPrompt() {

  const t =
    state.technical;

  const lastCandles =
    state.candles
      .slice(-30);

  return `
You are one analyst inside MT5 FOREX AI ANALYZER V12.

This is a financial-market analysis system.
Do NOT claim certainty.
Do NOT guarantee profit.
Do NOT invent missing market or news data.

Analyze the supplied market snapshot.

MARKET
Symbol: ${state.symbol}
Timeframe: ${state.timeframe}
Price: ${state.price}
Bid: ${state.bid}
Ask: ${state.ask}
Spread: ${state.spread}

TECHNICAL DATA
RSI(14): ${t.rsi}
EMA20: ${t.ema20}
EMA50: ${t.ema50}
EMA200: ${t.ema200}
ATR(14): ${t.atr}
Momentum: ${t.momentum}
Trend: ${t.trend}
Support: ${t.support}
Resistance: ${t.resistance}
Candle Structure: ${t.candleStructure}

NEWS
Event: ${state.news.event}
Currency: ${state.news.currency}
Impact: ${state.news.impact}
Actual: ${state.news.actual}
Forecast: ${state.news.forecast}
Previous: ${state.news.previous}
Release Time: ${state.news.releaseTime}
Status: ${state.news.status}

RECENT CANDLES
${JSON.stringify(lastCandles)}

Return ONLY valid JSON using exactly:

{
  "action": "BUY" | "SELL" | "WAIT",
  "confidence": 0,
  "reason": "short explanation"
}

Confidence must be an integer from 0 to 100.

If the evidence is mixed or insufficient, use WAIT.
`;
}

// ============================================================
// JSON EXTRACTION
// ============================================================

function extractJSON(text) {

  if (!text) {
    throw new Error(
      'Empty AI response'
    );
  }

  let cleaned =
    String(text)
      .trim()
      .replace(/^```json/i, '')
      .replace(/^```/i, '')
      .replace(/```$/i, '')
      .trim();

  try {

    return JSON.parse(
      cleaned
    );

  } catch (_) {

    const start =
      cleaned.indexOf('{');

    const end =
      cleaned.lastIndexOf('}');

    if (
      start !== -1 &&
      end > start
    ) {

      return JSON.parse(
        cleaned.slice(
          start,
          end + 1
        )
      );
    }

    throw new Error(
      'AI returned invalid JSON'
    );
  }
}

function normalizeAIResult(
  provider,
  model,
  raw,
  latency
) {

  const parsed =
    extractJSON(raw);

  const allowed =
    ['BUY', 'SELL', 'WAIT'];

  const action =
    allowed.includes(
      String(
        parsed.action || ''
      ).toUpperCase()
    )
      ? String(
          parsed.action
        ).toUpperCase()
      : 'WAIT';

  const confidence =
    clamp(
      Math.round(
        safeNumber(
          parsed.confidence
        ) || 0
      ),
      0,
      100
    );

  return {

    provider,

    status: 'ONLINE',

    action,

    confidence,

    reason:
      String(
        parsed.reason ||
        'No reason supplied.'
      ).slice(0, 500),

    model,

    latency,

    error: null,

    timestamp:
      nowISO()
  };
}

// ============================================================
// GENERIC OPENAI-COMPATIBLE CALL
// ============================================================

async function callOpenAICompatible({
  provider,
  apiKey,
  baseURL,
  model,
  prompt
}) {

  if (!apiKey) {

    throw new Error(
      `${provider} API key is not configured.`
    );
  }

  const started =
    Date.now();

  const response =
    await fetch(
      `${baseURL}/chat/completions`,
      {

        method: 'POST',

        headers: {

          'Content-Type':
            'application/json',

          Authorization:
            `Bearer ${apiKey}`
        },

        body:
          JSON.stringify({

            model,

            messages: [

              {
                role: 'system',

                content:
                  'You are a disciplined financial-market analyst. Return only the requested JSON.'
              },

              {
                role: 'user',

                content:
                  prompt
              }
            ],

            temperature: 0.1,

            max_tokens: 300
          })
      }
    );

  const latency =
    Date.now() -
    started;

  const text =
    await response.text();

  if (!response.ok) {

    throw new Error(
      `${provider} HTTP ${response.status}: ${text.slice(0, 500)}`
    );
  }

  let data;

  try {

    data =
      JSON.parse(text);

  } catch {

    throw new Error(
      `${provider} returned invalid API JSON.`
    );
  }

  const content =
    data?.choices?.[0]?.message?.content;

  if (!content) {

    throw new Error(
      `${provider} returned no message content.`
    );
  }

  return normalizeAIResult(
    provider,
    model,
    content,
    latency
  );
}

// ============================================================
// OPENAI
// ============================================================

async function callOpenAI(prompt) {

  /*
   * OpenAI currently supports the Responses API.
   * This implementation uses the Responses endpoint.
   */

  if (!OPENAI_API_KEY) {

    throw new Error(
      'OpenAI API key is not configured.'
    );
  }

  const started =
    Date.now();

  const response =
    await fetch(
      'https://api.openai.com/v1/responses',
      {

        method: 'POST',

        headers: {

          'Content-Type':
            'application/json',

          Authorization:
            `Bearer ${OPENAI_API_KEY}`
        },

        body:
          JSON.stringify({

            model:
              AI_MODELS.openai,

            input: [

              {
                role: 'system',

                content:
                  'You are a disciplined financial-market analyst. Return only valid JSON.'
              },

              {
                role: 'user',

                content:
                  buildAIPrompt()
              }
            ],

            max_output_tokens: 300
          })
      }
    );

  const latency =
    Date.now() -
    started;

  const text =
    await response.text();

  if (!response.ok) {

    throw new Error(
      `OpenAI HTTP ${response.status}: ${text.slice(0, 500)}`
    );
  }

  let data;

  try {

    data =
      JSON.parse(text);

  } catch {

    throw new Error(
      'OpenAI returned invalid API JSON.'
    );
  }

  const content =
    data.output_text ||
    data.output
      ?.flatMap(
        item =>
          item.content || []
      )
      ?.map(
        item =>
          item.text || ''
      )
      ?.join(' ');

  if (!content) {

    throw new Error(
      'OpenAI returned no text.'
    );
  }

  return normalizeAIResult(
    'OpenAI',
    AI_MODELS.openai,
    content,
    latency
  );
}

// ============================================================
// GEMINI
// ============================================================

async function callGemini(prompt) {

  if (!GEMINI_API_KEY) {

    throw new Error(
      'Gemini API key is not configured.'
    );
  }

  const started =
    Date.now();

  const url =
    `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      AI_MODELS.gemini
    )}:generateContent?key=${encodeURIComponent(
      GEMINI_API_KEY
    )}`;

  const response =
    await fetch(
      url,
      {

        method: 'POST',

        headers: {

          'Content-Type':
            'application/json'
        },

        body:
          JSON.stringify({

            systemInstruction: {

              parts: [

                {
                  text:
                    'You are a disciplined financial-market analyst. Return only valid JSON.'
                }
              ]
            },

            contents: [

              {
                role: 'user',

                parts: [

                  {
                    text: prompt
                  }
                ]
              }
            ],

            generationConfig: {

              temperature: 0.1,

              maxOutputTokens: 300,

              responseMimeType:
                'application/json'
            }
          })
      }
    );

  const latency =
    Date.now() -
    started;

  const text =
    await response.text();

  if (!response.ok) {

    throw new Error(
      `Gemini HTTP ${response.status}: ${text.slice(0, 500)}`
    );
  }

  let data;

  try {

    data =
      JSON.parse(text);

  } catch {

    throw new Error(
      'Gemini returned invalid API JSON.'
    );
  }

  const content =
    data?.candidates?.[0]
      ?.content
      ?.parts
      ?.map(
        p => p.text || ''
      )
      ?.join('');

  if (!content) {

    throw new Error(
      'Gemini returned no text.'
    );
  }

  return normalizeAIResult(
    'Gemini',
    AI_MODELS.gemini,
    content,
    latency
  );
}

// ============================================================
// DEEPSEEK
// ============================================================

async function callDeepSeek(prompt) {

  return callOpenAICompatible({

    provider:
      'DeepSeek',

    apiKey:
      DEEPSEEK_API_KEY,

    baseURL:
      'https://api.deepseek.com',

    model:
      AI_MODELS.deepseek,

    prompt
  });
}

// ============================================================
// GROQ
// ============================================================

async function callGroq(prompt) {

  return callOpenAICompatible({

    provider:
      'Groq',

    apiKey:
      GROQ_API_KEY,

    baseURL:
      'https://api.groq.com/openai/v1',

    model:
      AI_MODELS.groq,

    prompt
  });
}

// ============================================================
// OPENROUTER
// ============================================================

async function callOpenRouter(prompt) {

  return callOpenAICompatible({

    provider:
      'OpenRouter',

    apiKey:
      OPENROUTER_API_KEY,

    baseURL:
      'https://openrouter.ai/api/v1',

    model:
      AI_MODELS.openrouter,

    prompt
  });
}

// ============================================================
// MISTRAL
// ============================================================

async function callMistral(prompt) {

  return callOpenAICompatible({

    provider:
      'Mistral AI',

    apiKey:
      MISTRAL_API_KEY,

    baseURL:
      'https://api.mistral.ai/v1',

    model:
      AI_MODELS.mistral,

    prompt
  });
}

// ============================================================
// RUN ONE ANALYST SAFELY
// ============================================================

async function runOneAnalyst(
  key,
  functionCall
) {

  try {

    const result =
      await functionCall();

    state.ai.analysts[key] =
      result;

    return result;

  } catch (error) {

    const providerNames = {

      openai: 'OpenAI',

      deepseek: 'DeepSeek',

      gemini: 'Gemini',

      groq: 'Groq',

      openrouter: 'OpenRouter',

      mistral: 'Mistral AI'
    };

    const failed = {

      provider:
        providerNames[key],

      status: 'ERROR',

      action: 'WAIT',

      confidence: 0,

      reason:
        'AI provider unavailable.',

      model:
        AI_MODELS[key],

      latency: null,

      error:
        error.message,

      timestamp:
        nowISO()
    };

    state.ai.analysts[key] =
      failed;

    return failed;
  }
}

// ============================================================
// CONSENSUS ENGINE
// ============================================================

function calculateAIConsensus(
  results
) {

  const active =
    results.filter(
      r =>
        r &&
        r.status === 'ONLINE' &&
        ['BUY', 'SELL', 'WAIT']
          .includes(r.action)
    );

  let buyVotes = 0;
  let sellVotes = 0;
  let waitVotes = 0;

  let buyConfidence = 0;
  let sellConfidence = 0;
  let waitConfidence = 0;

  for (const result of active) {

    if (result.action === 'BUY') {

      buyVotes++;
      buyConfidence +=
        result.confidence;

    } else if (
      result.action === 'SELL'
    ) {

      sellVotes++;
      sellConfidence +=
        result.confidence;

    } else {

      waitVotes++;
      waitConfidence +=
        result.confidence;
    }
  }

  const total =
    active.length;

  if (total === 0) {

    state.ai.consensus = {

      action: 'WAIT',

      confidence: 0,

      agreement: 0,

      buy: 0,

      sell: 0,

      wait: 100,

      activeAnalysts: 0,

      totalAnalysts: 6,

      reason:
        'No AI provider returned a valid analysis.'
    };

    return;
  }

  const buyPct =
    Math.round(
      buyVotes / total * 100
    );

  const sellPct =
    Math.round(
      sellVotes / total * 100
    );

  const waitPct =
    Math.max(
      0,
      100 -
      buyPct -
      sellPct
    );

  const candidates = [

    {
      action: 'BUY',
      votes: buyVotes,
      confidence:
        buyVotes
          ? buyConfidence /
            buyVotes
          : 0
    },

    {
      action: 'SELL',
      votes: sellVotes,
      confidence:
        sellVotes
          ? sellConfidence /
            sellVotes
          : 0
    },

    {
      action: 'WAIT',
      votes: waitVotes,
      confidence:
        waitVotes
          ? waitConfidence /
            waitVotes
          : 0
    }
  ];

  candidates.sort(
    (a, b) =>
      b.votes - a.votes ||
      b.confidence -
        a.confidence
  );

  const winner =
    candidates[0];

  const agreement =
    Math.round(
      winner.votes /
      total *
      100
    );

  const confidence =
    Math.round(
      (
        winner.confidence *
        0.6
      ) +
      (
        agreement *
        0.4
      )
    );

  let reason =
    `${winner.votes}/${total} active AI analysts favor ${winner.action}.`;

  if (
    agreement < 67
  ) {

    reason +=
      ' Consensus is weak; WAIT is recommended until confirmation improves.';
  }

  state.ai.consensus = {

    action:
      winner.action,

    confidence:
      clamp(
        confidence,
        0,
        100
      ),

    agreement,

    buy:
      buyPct,

    sell:
      sellPct,

    wait:
      waitPct,

    activeAnalysts:
      total,

    totalAnalysts:
      6,

    reason
  };
}

// ============================================================
// AI + TECHNICAL FINAL DECISION
// ============================================================

function combineTechnicalAndAI() {

  const ai =
    state.ai.consensus;

  const technicalAction =
    state.decision.action;

  let action =
    'WAIT';

  /*
   * Conservative consensus rule:
   *
   * AI must have at least 67% agreement.
   * Technical engine must agree with AI.
   *
   * Otherwise WAIT.
   */

  if (
    ai.agreement >= 67 &&
    ai.confidence >= 60
  ) {

    if (
      ai.action === 'BUY' &&
      technicalAction === 'BUY'
    ) {

      action = 'BUY';

    } else if (
      ai.action === 'SELL' &&
      technicalAction === 'SELL'
    ) {

      action = 'SELL';
    }
  }

  const finalConfidence =
    action === 'WAIT'
      ? Math.min(
          ai.confidence,
          state.decision.confidence
        )
      : Math.round(
          (
            ai.confidence *
            0.60
          ) +
          (
            state.decision.confidence *
            0.40
          )
        );

  state.decision = {

    ...state.decision,

    action,

    confidence:
      clamp(
        finalConfidence,
        0,
        100
      ),

    agreement:
      ai.agreement,

    reason:
      action === 'BUY'
        ? 'AI consensus and technical analysis agree on BUY.'
        : action === 'SELL'
          ? 'AI consensus and technical analysis agree on SELL.'
          : 'AI and technical confirmation are insufficient. WAIT.'
  };

  state.forecast = {

    direction:
      action,

    confidence:
      state.decision.confidence,

    reason:
      state.decision.reason
  };
}

// ============================================================
// RUN ALL SIX AI PROVIDERS
// ============================================================

async function runAIConsensus() {

  if (state.ai.running) {

    return;
  }

  if (
    !state.connected ||
    state.price === null
  ) {

    broadcast({

      type: 'log',

      message:
        'AI scan skipped: waiting for live MT5 data.'
    });

    return;
  }

  state.ai.running = true;

  broadcast({

    type: 'aiStatus',

    data: state.ai
  });

  const prompt =
    buildAIPrompt();

  const tasks = [

    [
      'openai',

      () =>
        callOpenAI(prompt)
    ],

    [
      'deepseek',

      () =>
        callDeepSeek(prompt)
    ],

    [
      'gemini',

      () =>
        callGemini(prompt)
    ],

    [
      'groq',

      () =>
        callGroq(prompt)
    ],

    [
      'openrouter',

      () =>
        callOpenRouter(prompt)
    ],

    [
      'mistral',

      () =>
        callMistral(prompt)
    ]
  ];

  /*
   * Run in parallel.
   * One provider failing does NOT stop the others.
   */

  const results =
    await Promise.all(
      tasks.map(
        ([key, fn]) =>
          runOneAnalyst(
            key,
            fn
          )
      )
    );

  calculateAIConsensus(
    results
  );

  combineTechnicalAndAI();

  state.ai.running =
    false;

  state.ai.lastRun =
    nowISO();

  broadcast({

    type: 'analysis',

    data: {

      technical:
        state.technical,

      consensus:
        state.ai.consensus,

      decision:
        state.decision
    }
  });

  broadcast({

    type: 'ai',

    data:
      state.ai
  });

  broadcast({

    type: 'forecast',

    data:
      state.forecast
  });

  broadcast({

    type: 'log',

    message:
      `AI consensus completed: ${state.ai.consensus.action} (${state.ai.consensus.agreement}% agreement).`
  });
}

// ============================================================
// SAFETY ENGINE
// ============================================================

function runSafety() {

  const staleLimit =
    15000;

  const age =
    state.lastUpdate
      ? Date.now() -
        new Date(
          state.lastUpdate
        ).getTime()
      : Infinity;

  const staleData =
    age > staleLimit;

  const spreadOK =
    state.spread !== null &&
    state.spread <= 50;

  const newsRisk =
    state.news.impact ===
      'HIGH' &&
    state.news.status ===
      'UPCOMING';

  const safe =
    state.connected &&
    !staleData &&
    spreadOK &&
    !newsRisk;

  let message =
    'Safety checks passed.';

  if (!state.connected) {

    message =
      'MT5 is not connected.';

  } else if (staleData) {

    message =
      'Market data is stale.';

  } else if (!spreadOK) {

    message =
      'Spread check failed.';

  } else if (newsRisk) {

    message =
      'High-impact news risk detected.';
  }

  state.safety = {

    safe,

    spreadOK,

    staleData,

    newsRisk,

    riskOK: true,

    message
  };
}

// ============================================================
// MT5 DATA
// ============================================================

function normalizeCandle(c) {

  return {

    time:
      c.time ||
      c.timestamp ||
      Date.now(),

    open:
      safeNumber(c.open),

    high:
      safeNumber(c.high),

    low:
      safeNumber(c.low),

    close:
      safeNumber(c.close),

    volume:
      safeNumber(c.volume) || 0
  };
}

function updateFromMT5(data) {

  if (
    !data ||
    typeof data !== 'object'
  ) {

    return;
  }

  if (data.symbol) {

    state.symbol =
      String(data.symbol);
  }

  if (data.timeframe) {

    state.timeframe =
      String(data.timeframe);
  }

  const price =
    safeNumber(data.price) ??
    safeNumber(data.last);

  const bid =
    safeNumber(data.bid);

  const ask =
    safeNumber(data.ask);

  if (price !== null) {

    state.price =
      price;
  }

  if (bid !== null) {

    state.bid =
      bid;
  }

  if (ask !== null) {

    state.ask =
      ask;
  }

  if (
    bid !== null &&
    ask !== null
  ) {

    state.spread =
      round(
        Math.abs(
          ask - bid
        ),
        5
      );

  } else if (
    safeNumber(data.spread) !== null
  ) {

    state.spread =
      safeNumber(
        data.spread
      );
  }

  state.tickTime =
    data.time ||
    data.timestamp ||
    nowISO();

  state.lastUpdate =
    nowISO();

  state.connected =
    true;

  if (
    Array.isArray(
      data.candles
    )
  ) {

    state.candles =
      data.candles
        .slice(-500)
        .map(
          normalizeCandle
        );
  }

  if (data.candle) {

    state.candles.push(
      normalizeCandle(
        data.candle
      )
    );

    state.candles =
      state.candles
        .slice(-500);
  }

  calculateTechnicalAnalysis();

  runTechnicalDecision();

  runSafety();

  broadcast({

    type: 'tick',

    data: {

      price:
        state.price,

      bid:
        state.bid,

      ask:
        state.ask,

      spread:
        state.spread,

      time:
        state.tickTime,

      symbol:
        state.symbol
    }
  });

  broadcast({

    type: 'candles',

    data:
      state.candles
        .slice(-500)
  });

  broadcast({

    type: 'analysis',

    data: {

      technical:
        state.technical,

      consensus:
        state.ai.consensus,

      decision:
        state.decision
    }
  });

  broadcast({

    type: 'safety',

    data:
      state.safety
  });
}

// ============================================================
// MT5 PUSH
// ============================================================

app.post(
  '/api/mt5/push',
  (req, res) => {

    const secret =
      req.headers[
        'x-mt5-secret'
      ] ||
      req.body?.secret;

    if (
      !constantTimeEqual(
        secret,
        MT5_BRIDGE_SECRET
      )
    ) {

      return res
        .status(401)
        .json({

          ok: false,

          error:
            'Invalid MT5 bridge secret.'
        });
    }

    try {

      updateFromMT5(
        req.body
      );

      return res.json({

        ok: true,

        message:
          'MT5 data received.'
      });

    } catch (error) {

      console.error(
        'MT5 PUSH ERROR:',
        error
      );

      return res
        .status(400)
        .json({

          ok: false,

          error:
            'Invalid MT5 data.'
        });
    }
  }
);

// ============================================================
// LOGIN
// ============================================================

app.post(
  '/api/login',
  (req, res) => {

    const password =
      req.body?.password;

    if (
      !constantTimeEqual(
        password,
        BOT_PASSWORD
      )
    ) {

      return res
        .status(401)
        .json({

          ok: false,

          error:
            'Invalid password.'
        });
    }

    return res.json({

      ok: true,

      token:
        crypto
          .randomBytes(24)
          .toString('hex'),

      message:
        'Login successful.'
    });
  }
);

// ============================================================
// DASHBOARD
// ============================================================

app.get(
  '/api/dashboard',
  (req, res) => {

    runSafety();

    res.json({

      ok: true,

      data:
        publicState()
    });
  }
);

// ============================================================
// AI STATUS
// ============================================================

app.get(
  '/api/ai/status',
  (req, res) => {

    res.json({

      ok: true,

      providers:
        state.ai.analysts,

      consensus:
        state.ai.consensus,

      models:
        AI_MODELS
    });
  }
);

// ============================================================
// MANUAL AI SCAN
// ============================================================

app.post(
  '/api/ai/scan',
  async (req, res) => {

    try {

      await runAIConsensus();

      res.json({

        ok: true,

        data: {

          analysts:
            state.ai.analysts,

          consensus:
            state.ai.consensus,

          decision:
            state.decision
        }
      });

    } catch (error) {

      console.error(
        'AI SCAN ERROR:',
        error
      );

      res
        .status(500)
        .json({

          ok: false,

          error:
            error.message
        });
    }
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  '/health',
  (req, res) => {

    const configured = {

      openai:
        Boolean(
          OPENAI_API_KEY
        ),

      deepseek:
        Boolean(
          DEEPSEEK_API_KEY
        ),

      gemini:
        Boolean(
          GEMINI_API_KEY
        ),

      groq:
        Boolean(
          GROQ_API_KEY
        ),

      openrouter:
        Boolean(
          OPENROUTER_API_KEY
        ),

      mistral:
        Boolean(
          MISTRAL_API_KEY
        )
    };

    res.json({

      ok: true,

      service:
        'MT5 FOREX AI ANALYZER V12',

      mt5Connected:
        state.connected,

      aiProviders:
        configured,

      activeAI:
        Object.values(
          state.ai.analysts
        ).filter(
          a =>
            a.status ===
            'ONLINE'
        ).length,

      lastUpdate:
        state.lastUpdate,

      uptime:
        process.uptime(),

      time:
        nowISO()
    });
  }
);

// ============================================================
// WEBSOCKET
// ============================================================

wss.on(
  'connection',
  ws => {

    state.clients.add(ws);

    console.log(
      `Dashboard connected. Clients: ${state.clients.size}`
    );

    send(ws, {

      type:
        'connected',

      data:
        publicState()
    });

    ws.on(
      'message',
      async raw => {

        let message;

        try {

          message =
            JSON.parse(
              raw.toString()
            );

        } catch {

          send(ws, {

            type:
              'error',

            message:
              'Invalid JSON message.'
          });

          return;
        }

        await handleClientMessage(
          ws,
          message
        );
      }
    );

    ws.on(
      'close',
      () => {

        state.clients.delete(ws);

        console.log(
          `Dashboard disconnected. Clients: ${state.clients.size}`
        );
      }
    );

    ws.on(
      'error',
      error => {

        console.error(
          'WebSocket error:',
          error.message
        );
      }
    );
  }
);

// ============================================================
// CLIENT COMMANDS
// ============================================================

async function handleClientMessage(
  ws,
  message
) {

  switch (
    message.type
  ) {

    case 'subscribe': {

      if (message.symbol) {

        state.symbol =
          String(
            message.symbol
          );
      }

      if (message.timeframe) {

        state.timeframe =
          String(
            message.timeframe
          );
      }

      if (message.mode) {

        state.mode =
          String(
            message.mode
          );
      }

      send(ws, {

        type:
          'connected',

        data:
          publicState()
      });

      broadcast({

        type:
          'log',

        message:
          `Subscribed to ${state.symbol} ${state.timeframe}.`
      });

      break;
    }

    case 'scan': {

      calculateTechnicalAnalysis();

      runTechnicalDecision();

      runSafety();

      await runAIConsensus();

      break;
    }

    case 'stop': {

      state.decision = {

        ...state.decision,

        action:
          'WAIT',

        confidence:
          0,

        reason:
          'Scanner stopped.'
      };

      broadcast({

        type:
          'analysis',

        data: {

          technical:
            state.technical,

          consensus:
            state.ai.consensus,

          decision:
            state.decision
        }
      });

      break;
    }

    case 'ai_scan': {

      await runAIConsensus();

      break;
    }

    case 'alert': {

      broadcast({

        type:
          'log',

        message:
          message.message ||
          'Analyzer alert.'
      });

      break;
    }

    case 'paper_trade': {

      if (
        state.price === null
      ) {

        send(ws, {

          type:
            'error',

          message:
            'No live MT5 price available.'
        });

        return;
      }

      const direction =
        message.action ||
        state.decision.action;

      if (
        !['BUY', 'SELL']
          .includes(direction)
      ) {

        send(ws, {

          type:
            'error',

          message:
            'Paper trade requires BUY or SELL.'
        });

        return;
      }

      const paperPosition = {

        id:
          `PAPER-${Date.now()}`,

        symbol:
          state.symbol,

        type:
          direction,

        volume:
          safeNumber(
            message.volume
          ) || 0.01,

        entry:
          state.price,

        stopLoss:
          state.decision.stopLoss,

        takeProfit:
          state.decision.takeProfit,

        status:
          'PAPER',

        openedAt:
          nowISO()
      };

      state.positions.push(
        paperPosition
      );

      broadcast({

        type:
          'positions',

        data:
          state.positions
      });

      break;
    }

    case 'execute': {

      /*
       * Real execution deliberately remains disabled.
       *
       * The AI system can analyze and produce a signal,
       * but this gateway will not place a live broker order.
       */

      send(ws, {

        type:
          'error',

        message:
          'LIVE EXECUTION is locked. The current V12 gateway is analysis/paper-trading only.'
      });

      break;
    }

    default: {

      send(ws, {

        type:
          'error',

        message:
          `Unknown command: ${message.type}`
      });
    }
  }
}

// ============================================================
// AUTOMATIC AI REFRESH
// ============================================================

let lastAutomaticAI =
  0;

setInterval(
  async () => {

    if (
      !state.connected ||
      state.price === null ||
      state.ai.running
    ) {

      return;
    }

    const now =
      Date.now();

    /*
     * AI requests are intentionally throttled.
     * Do not call six providers on every tick.
     */

    if (
      now -
      lastAutomaticAI <
      60000
    ) {

      return;
    }

    lastAutomaticAI =
      now;

    try {

      await runAIConsensus();

    } catch (error) {

      console.error(
        'Automatic AI error:',
        error.message
      );
    }

  },
  5000
);

// ============================================================
// CONNECTION WATCHDOG
// ============================================================

setInterval(
  () => {

    runSafety();

    const stale =
      !state.lastUpdate ||
      Date.now() -
        new Date(
          state.lastUpdate
        ).getTime() >
        15000;

    if (
      stale &&
      state.connected
    ) {

      state.connected =
        false;

      state.safety = {

        safe:
          false,

        spreadOK:
          false,

        staleData:
          true,

        newsRisk:
          false,

        riskOK:
          true,

        message:
          'MT5 data connection is stale.'
      };

      broadcast({

        type:
          'error',

        message:
          'MT5 live feed disconnected or stale.'
      });

      broadcast({

        type:
          'safety',

        data:
          state.safety
      });
    }

  },
  5000
);

// ============================================================
// FALLBACK PAGE
// ============================================================

app.get(
  '*',
  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        'public',
        'index.html'
      )
    );
  }
);

// ============================================================
// START
// ============================================================

server.listen(
  PORT,
  () => {

    console.log('');
    console.log(
      '=============================================='
    );

    console.log(
      '🔥 MT5 FOREX AI ANALYZER V12'
    );

    console.log(
      'POWERED BY ELISY ANALYSIS DEVELOPER'
    );

    console.log(
      '=============================================='
    );

    console.log(
      `🚀 Port: ${PORT}`
    );

    console.log(
      '📡 WebSocket: /ws'
    );

    console.log(
      '📊 Dashboard: /'
    );

    console.log(
      '❤️ Health: /health'
    );

    console.log(
      '📥 MT5 Push: /api/mt5/push'
    );

    console.log(
      '🤖 AI Providers: 6'
    );

    console.log(
      '=============================================='
    );
  }
);
