'use strict';

const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT) || 10000;
const PUBLIC_DIR = path.join(__dirname, 'public');
const INDEX_FILE = path.join(PUBLIC_DIR, 'index.html');

const wss = new WebSocket.Server({
  server,
  path: '/ws'
});

// ============================================================
// CONFIG
// ============================================================

const BOT_PASSWORD =
  process.env.BOT_PASSWORD || 'change-this-password';

const MT5_BRIDGE_SECRET =
  process.env.MT5_BRIDGE_SECRET || 'change-this-secret';

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

const API_KEYS = {
  openai:
    process.env.OPENAI_API_KEY || '',

  deepseek:
    process.env.DEEPSEEK_API_KEY || '',

  gemini:
    process.env.GEMINI_API_KEY || '',

  groq:
    process.env.GROQ_API_KEY || '',

  openrouter:
    process.env.OPENROUTER_API_KEY || '',

  mistral:
    process.env.MISTRAL_API_KEY || ''
};

const MAX_CANDLES =
  Number(process.env.MAX_CANDLES) || 500;

const STALE_DATA_MS =
  Number(process.env.STALE_DATA_MS) || 15000;

const MAX_SPREAD_POINTS =
  Number(process.env.MAX_SPREAD_POINTS) || 50;

const AI_REFRESH_MS =
  Number(process.env.AI_REFRESH_MS) || 60000;

// ============================================================
// EXPRESS
// ============================================================

app.disable('x-powered-by');

app.use(
  express.json({
    limit: '2mb'
  })
);

app.use(
  express.urlencoded({
    extended: false,
    limit: '1mb'
  })
);

// Security headers
app.use((req, res, next) => {
  res.setHeader(
    'X-Content-Type-Options',
    'nosniff'
  );

  res.setHeader(
    'X-Frame-Options',
    'SAMEORIGIN'
  );

  res.setHeader(
    'Referrer-Policy',
    'strict-origin-when-cross-origin'
  );

  next();
});

// ============================================================
// GLOBAL STATE
// ============================================================

function emptyAnalyst(name) {
  return {
    provider: name,
    status: 'WAITING',
    action: 'WAIT',
    confidence: 0,
    reason: 'AI analysis has not started.',
    model: null,
    latency: null,
    error: null,
    timestamp: null
  };
}

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
      reason: 'Waiting for AI analysis.'
    }
  },

  forecast: {
    direction: 'WAIT',
    confidence: 0,
    reason: 'Waiting for AI analysis.'
  },

  decision: {
    action: 'WAIT',
    confidence: 0,
    agreement: 0,
    entry: null,
    stopLoss: null,
    takeProfit: null,
    reason: 'Waiting for live market data.'
  },

  safety: {
    safe: false,
    spreadOK: false,
    staleData: true,
    newsRisk: false,
    riskOK: true,
    message: 'Waiting for MT5 connection.'
  },

  positions: [],

  lastUpdate: null,

  clients: new Set()
};

// ============================================================
// HELPERS
// ============================================================

function safeNumber(value) {
  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : null;
}

function round(value, digits = 2) {
  const n = Number(value);

  if (!Number.isFinite(n)) {
    return null;
  }

  return Number(
    n.toFixed(digits)
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
  const aa = Buffer.from(
    String(a || '')
  );

  const bb = Buffer.from(
    String(b || '')
  );

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
    try {
      ws.send(
        JSON.stringify(payload)
      );
    } catch (error) {
      console.error(
        'WS SEND ERROR:',
        error.message
      );
    }
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
      try {
        ws.send(message);
      } catch (_) {}
    }
  }
}

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
      state.candles.slice(-MAX_CANDLES),

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
      (
        (values[i] - ema) *
        multiplier
      ) + ema;
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
    const diff =
      values[i] -
      values[i - 1];

    if (diff >= 0) {
      gains += diff;
    } else {
      losses += Math.abs(diff);
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
    const diff =
      values[i] -
      values[i - 1];

    const gain =
      Math.max(diff, 0);

    const loss =
      Math.max(-diff, 0);

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

  return 100 - 100 / (1 + rs);
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

    ranges.push(
      Math.max(
        high - low,
        Math.abs(
          high - previousClose
        ),
        Math.abs(
          low - previousClose
        )
      )
    );
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

  if (candles.length < 20) {
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

  if (closes.length < 20) {
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

  if (
    previous !== 0
  ) {
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
      trend = 'BULLISH';
    } else if (
      last < ema20 &&
      ema20 < ema50
    ) {
      trend = 'BEARISH';
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
      safeNumber(lastCandle.open);

    const high =
      safeNumber(lastCandle.high);

    const low =
      safeNumber(lastCandle.low);

    const close =
      safeNumber(lastCandle.close);

    if (
      open !== null &&
      high !== null &&
      low !== null &&
      close !== null
    ) {
      const body =
        Math.abs(close - open);

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

  let action = 'WAIT';

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
    }

    if (action === 'SELL') {
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

  return `
You are one analyst inside MT5 FOREX AI ANALYZER V12.

This system provides market analysis only.
Do not guarantee profit.
Do not claim certainty.
Do not invent missing market or news data.

Analyze the supplied snapshot.

MARKET
Symbol: ${state.symbol}
Timeframe: ${state.timeframe}
Price: ${state.price}
Bid: ${state.bid}
Ask: ${state.ask}
Spread: ${state.spread}

TECHNICAL
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
${JSON.stringify(
  state.candles.slice(-30)
)}

Return ONLY valid JSON:

{
  "action": "BUY",
  "confidence": 0,
  "reason": "short explanation"
}

Action must be BUY, SELL, or WAIT.
Confidence must be an integer from 0 to 100.

If evidence is mixed or insufficient, return WAIT.
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

  const cleaned =
    String(text)
      .trim()
      .replace(/^```json/i, '')
      .replace(/^```/i, '')
      .replace(/```$/i, '')
      .trim();

  try {
    return JSON.parse(cleaned);
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

  const actionText =
    String(
      parsed.action || 'WAIT'
    ).toUpperCase();

  const action =
    ['BUY', 'SELL', 'WAIT']
      .includes(actionText)
      ? actionText
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
// FETCH WITH TIMEOUT
// ============================================================

async function fetchWithTimeout(
  url,
  options = {},
  timeout = 30000
) {
  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () => controller.abort(),
      timeout
    );

  try {
    return await fetch(
      url,
      {
        ...options,
        signal:
          controller.signal
      }
    );
  } finally {
    clearTimeout(timer);
  }
}

// ============================================================
// OPENAI COMPATIBLE
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
    await fetchWithTimeout(
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
                  'You are a disciplined financial-market analyst. Return only valid JSON.'
              },
              {
                role: 'user',
                content: prompt
              }
            ],

            temperature: 0.1,

            max_tokens: 300
          })
      },
      30000
    );

  const latency =
    Date.now() - started;

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `${provider} HTTP ${response.status}: ${text.slice(0, 300)}`
    );
  }

  let data;

  try {
    data = JSON.parse(text);
  } catch (_) {
    throw new Error(
      `${provider} returned invalid API JSON.`
    );
  }

  const content =
    data?.choices?.[0]
      ?.message?.content;

  if (!content) {
    throw new Error(
      `${provider} returned no content.`
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
  if (!API_KEYS.openai) {
    throw new Error(
      'OpenAI API key is not configured.'
    );
  }

  const started =
    Date.now();

  const response =
    await fetchWithTimeout(
      'https://api.openai.com/v1/responses',
      {
        method: 'POST',

        headers: {
          'Content-Type':
            'application/json',

          Authorization:
            `Bearer ${API_KEYS.openai}`
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
                content: prompt
              }
            ],

            max_output_tokens: 300
          })
      },
      30000
    );

  const latency =
    Date.now() - started;

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `OpenAI HTTP ${response.status}: ${text.slice(0, 300)}`
    );
  }

  let data;

  try {
    data = JSON.parse(text);
  } catch (_) {
    throw new Error(
      'OpenAI returned invalid API JSON.'
    );
  }

  let content =
    data.output_text || '';

  if (!content && Array.isArray(data.output)) {
    content =
      data.output
        .flatMap(
          item =>
            Array.isArray(item.content)
              ? item.content
              : []
        )
        .map(
          item =>
            item.text || ''
        )
        .join('');
  }

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
  if (!API_KEYS.gemini) {
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
      API_KEYS.gemini
    )}`;

  const response =
    await fetchWithTimeout(
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
      },
      30000
    );

  const latency =
    Date.now() - started;

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `Gemini HTTP ${response.status}: ${text.slice(0, 300)}`
    );
  }

  let data;

  try {
    data = JSON.parse(text);
  } catch (_) {
    throw new Error(
      'Gemini returned invalid API JSON.'
    );
  }

  const content =
    data?.candidates?.[0]
      ?.content?.parts
      ?.map(
        part => part.text || ''
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
// SIX PROVIDERS
// ============================================================

async function callDeepSeek(prompt) {
  return callOpenAICompatible({
    provider: 'DeepSeek',
    apiKey: API_KEYS.deepseek,
    baseURL:
      'https://api.deepseek.com',
    model:
      AI_MODELS.deepseek,
    prompt
  });
}

async function callGroq(prompt) {
  return callOpenAICompatible({
    provider: 'Groq',
    apiKey: API_KEYS.groq,
    baseURL:
      'https://api.groq.com/openai/v1',
    model:
      AI_MODELS.groq,
    prompt
  });
}

async function callOpenRouter(prompt) {
  return callOpenAICompatible({
    provider: 'OpenRouter',
    apiKey: API_KEYS.openrouter,
    baseURL:
      'https://openrouter.ai/api/v1',
    model:
      AI_MODELS.openrouter,
    prompt
  });
}

async function callMistral(prompt) {
  return callOpenAICompatible({
    provider: 'Mistral AI',
    apiKey: API_KEYS.mistral,
    baseURL:
      'https://api.mistral.ai/v1',
    model:
      AI_MODELS.mistral,
    prompt
  });
}

// ============================================================
// SAFE ANALYST
// ============================================================

async function runOneAnalyst(
  key,
  functionCall
) {
  const names = {
    openai: 'OpenAI',
    deepseek: 'DeepSeek',
    gemini: 'Gemini',
    groq: 'Groq',
    openrouter: 'OpenRouter',
    mistral: 'Mistral AI'
  };

  try {
    const result =
      await functionCall();

    state.ai.analysts[key] =
      result;

    return result;
  } catch (error) {
    const failed = {
      provider:
        names[key],

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
// CONSENSUS
// ============================================================

function calculateAIConsensus(results) {
  const active =
    results.filter(
      result =>
        result &&
        result.status === 'ONLINE'
    );

  if (!active.length) {
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

  let buyVotes = 0;
  let sellVotes = 0;
  let waitVotes = 0;

  let buyConfidence = 0;
  let sellConfidence = 0;
  let waitConfidence = 0;

  for (
    const result of active
  ) {
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
      winner.confidence * 0.6 +
      agreement * 0.4
    );

  const buy =
    Math.round(
      buyVotes / total * 100
    );

  const sell =
    Math.round(
      sellVotes / total * 100
    );

  const wait =
    Math.max(
      0,
      100 - buy - sell
    );

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

    buy,

    sell,

    wait,

    activeAnalysts:
      total,

    totalAnalysts:
      6,

    reason:
      `${winner.votes}/${total} active AI analysts favor ${winner.action}.`
  };
}

// ============================================================
// FINAL DECISION
// ============================================================

function combineTechnicalAndAI() {
  const ai =
    state.ai.consensus;

  const technical =
    state.decision.action;

  let action = 'WAIT';

  if (
    ai.agreement >= 67 &&
    ai.confidence >= 60
  ) {
    if (
      ai.action === 'BUY' &&
      technical === 'BUY'
    ) {
      action = 'BUY';
    }

    if (
      ai.action === 'SELL' &&
      technical === 'SELL'
    ) {
      action = 'SELL';
    }
  }

  const confidence =
    action === 'WAIT'
      ? Math.min(
          ai.confidence,
          state.decision.confidence
        )
      : Math.round(
          ai.confidence * 0.6 +
          state.decision.confidence * 0.4
        );

  state.decision = {
    ...state.decision,

    action,

    confidence:
      clamp(
        confidence,
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
// AI SCAN
// ============================================================

async function runAIConsensus() {
  if (state.ai.running) {
    return;
  }

  if (
    !state.connected ||
    state.price === null
  ) {
    return;
  }

  state.ai.running = true;

  broadcast({
    type: 'aiStatus',
    data: state.ai
  });

  try {
    const prompt =
      buildAIPrompt();

    const tasks = [
      [
        'openai',
        () => callOpenAI(prompt)
      ],

      [
        'deepseek',
        () => callDeepSeek(prompt)
      ],

      [
        'gemini',
        () => callGemini(prompt)
      ],

      [
        'groq',
        () => callGroq(prompt)
      ],

      [
        'openrouter',
        () => callOpenRouter(prompt)
      ],

      [
        'mistral',
        () => callMistral(prompt)
      ]
    ];

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
  } catch (error) {
    console.error(
      'AI CONSENSUS ERROR:',
      error
    );
  } finally {
    state.ai.running =
      false;
  }
}

// ============================================================
// SAFETY
// ============================================================

function runSafety() {
  const age =
    state.lastUpdate
      ? Date.now() -
        new Date(
          state.lastUpdate
        ).getTime()
      : Infinity;

  const staleData =
    age > STALE_DATA_MS;

  const spreadOK =
    state.spread !== null &&
    state.spread <=
      MAX_SPREAD_POINTS;

  const newsRisk =
    state.news.impact === 'HIGH' &&
    state.news.status === 'UPCOMING';

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
// MT5 NORMALIZATION
// ============================================================

function normalizeCandle(c) {
  return {
    time:
      c?.time ||
      c?.timestamp ||
      Date.now(),

    open:
      safeNumber(c?.open),

    high:
      safeNumber(c?.high),

    low:
      safeNumber(c?.low),

    close:
      safeNumber(c?.close),

    volume:
      safeNumber(c?.volume) || 0
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
    state.price = price;
  }

  if (bid !== null) {
    state.bid = bid;
  }

  if (ask !== null) {
    state.ask = ask;
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
  } else {
    const suppliedSpread =
      safeNumber(
        data.spread
      );

    if (
      suppliedSpread !== null
    ) {
      state.spread =
        suppliedSpread;
    }
  }

  state.tickTime =
    data.time ||
    data.timestamp ||
    nowISO();

  state.lastUpdate =
    nowISO();

  state.connected = true;

  if (
    Array.isArray(
      data.candles
    )
  ) {
    state.candles =
      data.candles
        .slice(-MAX_CANDLES)
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
      state.candles.slice(
        -MAX_CANDLES
      );
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
      state.candles.slice(
        -MAX_CANDLES
      )
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
      req.headers['x-mt5-secret'] ||
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
          success: false,
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
        success: true,
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
          success: false,
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
    try {
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
            success: false,
            message:
              'Invalid password.',
            error:
              'Invalid password.'
          });
      }

      const token =
        crypto
          .randomBytes(32)
          .toString('hex');

      return res.json({
        ok: true,
        success: true,
        authenticated: true,
        token,
        message:
          'Login successful.'
      });
    } catch (error) {
      console.error(
        'LOGIN ERROR:',
        error
      );

      return res
        .status(500)
        .json({
          ok: false,
          success: false,
          message:
            'Login server error.'
        });
    }
  }
);

// ============================================================
// DASHBOARD API
// ============================================================

app.get(
  '/api/dashboard',
  (req, res) => {
    try {
      runSafety();

      return res.json({
        ok: true,
        success: true,
        data:
          publicState()
      });
    } catch (error) {
      console.error(
        'DASHBOARD ERROR:',
        error
      );

      return res
        .status(500)
        .json({
          ok: false,
          success: false,
          error:
            'Dashboard data error.'
        });
    }
  }
);

// ============================================================
// AI STATUS
// ============================================================

app.get(
  '/api/ai/status',
  (req, res) => {
    return res.json({
      ok: true,
      success: true,

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

      return res.json({
        ok: true,
        success: true,

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

      return res
        .status(500)
        .json({
          ok: false,
          success: false,
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
          API_KEYS.openai
        ),

      deepseek:
        Boolean(
          API_KEYS.deepseek
        ),

      gemini:
        Boolean(
          API_KEYS.gemini
        ),

      groq:
        Boolean(
          API_KEYS.groq
        ),

      openrouter:
        Boolean(
          API_KEYS.openrouter
        ),

      mistral:
        Boolean(
          API_KEYS.mistral
        )
    };

    return res.json({
      ok: true,
      success: true,

      service:
        'MT5 FOREX AI ANALYZER V12',

      version:
        '12.0.3',

      mt5Connected:
        state.connected,

      aiProviders:
        configured,

      activeAI:
        Object.values(
          state.ai.analysts
        ).filter(
          analyst =>
            analyst.status ===
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
      `📱 Dashboard connected. Clients: ${state.clients.size}`
    );

    send(ws, {
      type: 'connected',
      data:
        publicState()
    });

    ws.on(
      'message',
      async raw => {
        try {
          const message =
            JSON.parse(
              raw.toString()
            );

          await handleClientMessage(
            ws,
            message
          );
        } catch (error) {
          send(ws, {
            type: 'error',
            message:
              error.message ||
              'Invalid WebSocket message.'
          });
        }
      }
    );

    ws.on(
      'close',
      () => {
        state.clients.delete(ws);

        console.log(
          `📱 Dashboard disconnected. Clients: ${state.clients.size}`
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
  if (
    !message ||
    typeof message !== 'object'
  ) {
    send(ws, {
      type: 'error',
      message:
        'Invalid command.'
    });

    return;
  }

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
        type: 'connected',
        data:
          publicState()
      });

      broadcast({
        type: 'log',
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

    case 'ai_scan': {
      await runAIConsensus();

      break;
    }

    case 'stop': {
      state.decision = {
        ...state.decision,

        action: 'WAIT',

        confidence: 0,

        reason:
          'Scanner stopped.'
      };

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

      break;
    }

    case 'alert': {
      broadcast({
        type: 'log',

        message:
          String(
            message.message ||
            'Analyzer alert.'
          ).slice(0, 500)
      });

      break;
    }

    case 'paper_trade': {
      if (
        state.price === null
      ) {
        send(ws, {
          type: 'error',
          message:
            'No live MT5 price available.'
        });

        return;
      }

      const direction =
        String(
          message.action ||
          state.decision.action
        ).toUpperCase();

      if (
        !['BUY', 'SELL']
          .includes(direction)
      ) {
        send(ws, {
          type: 'error',
          message:
            'Paper trade requires BUY or SELL.'
        });

        return;
      }

      const position = {
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
        position
      );

      state.positions =
        state.positions.slice(-100);

      broadcast({
        type: 'positions',

        data:
          state.positions
      });

      break;
    }

    case 'execute': {
      send(ws, {
        type: 'error',

        message:
          'LIVE EXECUTION is locked. V12 is analysis/paper-trading only.'
      });

      break;
    }

    default: {
      send(ws, {
        type: 'error',

        message:
          `Unknown command: ${message.type}`
      });
    }
  }
}

// ============================================================
// AUTOMATIC AI REFRESH
// ============================================================

let lastAutomaticAI = 0;

setInterval(
  async () => {
    if (
      !state.connected ||
      state.price === null ||
      state.ai.running
    ) {
      return;
    }

    const current =
      Date.now();

    if (
      current -
      lastAutomaticAI <
      AI_REFRESH_MS
    ) {
      return;
    }

    lastAutomaticAI =
      current;

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
    try {
      runSafety();

      const stale =
        !state.lastUpdate ||
        Date.now() -
          new Date(
            state.lastUpdate
          ).getTime() >
          STALE_DATA_MS;

      if (
        stale &&
        state.connected
      ) {
        state.connected =
          false;

        state.safety = {
          safe: false,
          spreadOK: false,
          staleData: true,
          newsRisk: false,
          riskOK: true,
          message:
            'MT5 data connection is stale.'
        };

        broadcast({
          type: 'error',
          message:
            'MT5 live feed disconnected or stale.'
        });

        broadcast({
          type: 'safety',
          data:
            state.safety
        });
      }
    } catch (error) {
      console.error(
        'WATCHDOG ERROR:',
        error.message
      );
    }
  },
  5000
);

// ============================================================
// ROOT PAGE
// ============================================================

app.get(
  '/',
  (req, res) => {
    res.setHeader(
      'Cache-Control',
      'no-store, no-cache, must-revalidate, proxy-revalidate'
    );

    res.setHeader(
      'Pragma',
      'no-cache'
    );

    res.setHeader(
      'Expires',
      '0'
    );

    res.sendFile(
      INDEX_FILE,
      error => {
        if (error) {
          console.error(
            'INDEX FILE ERROR:',
            error
          );

          if (!res.headersSent) {
            res
              .status(500)
              .type('text/plain')
              .send(
                'MT5 FOREX AI ANALYZER V12: public/index.html was not found.'
              );
          }
        }
      }
    );
  }
);

// ============================================================
// STATIC FILES
// ============================================================

app.use(
  express.static(
    PUBLIC_DIR,
    {
      index: false,

      setHeaders: res => {
        res.setHeader(
          'Cache-Control',
          'no-store'
        );
      }
    }
  )
);

// ============================================================
// SAFE FALLBACK
// ============================================================

app.use(
  (req, res) => {
    if (
      req.method !== 'GET'
    ) {
      return res
        .status(404)
        .json({
          ok: false,
          success: false,
          error:
            'Route not found.'
        });
    }

    res.sendFile(
      INDEX_FILE,
      error => {
        if (error) {
          console.error(
            'FALLBACK ERROR:',
            error
          );

          if (!res.headersSent) {
            res
              .status(404)
              .type('text/plain')
              .send(
                'Dashboard file not found.'
              );
          }
        }
      }
    );
  }
);

// ============================================================
// ERROR HANDLER
// ============================================================

app.use(
  (error, req, res, next) => {
    console.error(
      'SERVER ERROR:',
      error
    );

    if (
      res.headersSent
    ) {
      return next(error);
    }

    res
      .status(500)
      .json({
        ok: false,
        success: false,
        error:
          'Internal server error.'
      });
  }
);

// ============================================================
// START
// ============================================================

server.listen(
  PORT,
  '0.0.0.0',
  () => {
    console.log('');
    console.log(
      '=============================================='
    );

    console.log(
      '🔥 MT5 FOREX AI ANALYZER V12.0.3'
    );

    console.log(
      'POWERED BY ELISY ANALYSIS DEVELOPER'
    );

    console.log(
      '=============================================='
    );

    console.log(
      `🚀 PORT: ${PORT}`
    );

    console.log(
      '📊 DASHBOARD: /'
    );

    console.log(
      '❤️ HEALTH: /health'
    );

    console.log(
      '📡 WEBSOCKET: /ws'
    );

    console.log(
      '📥 MT5 PUSH: /api/mt5/push'
    );

    console.log(
      '🤖 AI PROVIDERS: 6'
    );

    console.log(
      `📁 PUBLIC: ${PUBLIC_DIR}`
    );

    console.log(
      `📄 INDEX: ${INDEX_FILE}`
    );

    console.log(
      '=============================================='
    );
  }
);

// ============================================================
// PROCESS SAFETY
// ============================================================

process.on(
  'uncaughtException',
  error => {
    console.error(
      'UNCAUGHT EXCEPTION:',
      error
    );
  }
);

process.on(
  'unhandledRejection',
  reason => {
    console.error(
      'UNHANDLED REJECTION:',
      reason
    );
  }
);
