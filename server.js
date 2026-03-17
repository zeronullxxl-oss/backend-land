const express = require('express');
const path    = require('path');
const crypto  = require('crypto');
const https   = require('https');
const { Pool } = require('pg');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── PostgreSQL ────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://backend_land_db_user:0twG7HkUFoGS70iYTMnt82wekfKwliYL@dpg-d6ooqns50q8c739tg17g-a.oregon-postgres.render.com/backend_land_db',
  ssl: { rejectUnauthorized: false }
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS leads (
      id TEXT PRIMARY KEY,
      name TEXT, email TEXT, phone TEXT, buyer TEXT,
      ip TEXT, ua TEXT, country TEXT, city TEXT, flag TEXT,
      status TEXT DEFAULT 'pending',
      utms JSONB DEFAULT '{}',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS visits (
      id SERIAL PRIMARY KEY,
      count INTEGER DEFAULT 0
    );
    INSERT INTO visits (id, count) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
    CREATE TABLE IF NOT EXISTS pixels (
      pixel_id TEXT PRIMARY KEY,
      access_token TEXT NOT NULL,
      buyer TEXT NOT NULL,
      label TEXT DEFAULT '',
      test_code TEXT DEFAULT '',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS buyers (
      login TEXT PRIMARY KEY,
      password TEXT NOT NULL,
      name TEXT NOT NULL,
      geo TEXT DEFAULT '',
      sees TEXT NOT NULL,
      buyer_id SERIAL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`ALTER TABLE pixels ADD COLUMN IF NOT EXISTS test_code TEXT DEFAULT ''`).catch(()=>{});
  await pool.query(`ALTER TABLE buyers ADD COLUMN IF NOT EXISTS buyer_id SERIAL`).catch(()=>{});

  // Seed buyers — только если таблица пустая
  const { rows: bRows } = await pool.query(`SELECT COUNT(*) FROM buyers`);
  if (parseInt(bRows[0].count) === 0) {
    const buyerSeeds = [
      ['gpttrade', 'pass123', 'pumba',    'MX', 'pumba',    1],
      ['nepravda', 'pass456', 'nepravda', 'MX', 'nepravda', 2],
    ];
    for (const [login, password, name, geo, sees, bid] of buyerSeeds) {
      await pool.query(`INSERT INTO buyers (login, password, name, geo, sees, buyer_id) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
        [login, password, name, geo, sees, bid]);
    }
    // Обновляем sequence чтобы следующий buyer_id начинался с 3
    await pool.query(`SELECT setval(pg_get_serial_sequence('buyers','buyer_id'), GREATEST((SELECT MAX(buyer_id) FROM buyers), 2))`);
    console.log('[DB] buyers seeded');
  }

  // Seed pixels — только если таблица пустая
  const { rows: pRows } = await pool.query(`SELECT COUNT(*) FROM pixels`);
  if (parseInt(pRows[0].count) === 0) {
    const seeds = [
      ['3259080404253744', process.env.FB_TOKEN_PUMBA,      'pumba',    'pumba основной'],
      ['1237424481354094', process.env.FB_TOKEN_NEPRAVDA_1, 'nepravda', 'nepravda pixel 1'],
      ['711965651910969',  process.env.FB_TOKEN_NEPRAVDA_2, 'nepravda', 'nepravda pixel 2'],
      ['4328533490710973', process.env.FB_TOKEN_NEPRAVDA_3, 'nepravda', 'Mogilko'],
      ['4277414619193503', process.env.FB_TOKEN_NEPRAVDA_4, 'nepravda', 'GPT'],
    ];
    for (const [pid, token, buyer, label] of seeds) {
      if (token) {
        await pool.query(`INSERT INTO pixels (pixel_id, access_token, buyer, label) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
          [pid, token, buyer, label]);
      }
    }
    console.log('[DB] pixels seeded from ENV');
  }

  await refreshBuyersCache();
  await refreshPixelCache();
  console.log('[DB] initialized');
}

// ── Байеры — динамический кеш из БД ─────────────────────
let BUYERS = {}; // login -> { password, name, geo, sees, buyer_id }

async function refreshBuyersCache() {
  try {
    const { rows } = await pool.query(`SELECT login, password, name, geo, sees, buyer_id FROM buyers`);
    const newBuyers = {};
    rows.forEach(r => {
      newBuyers[r.login] = { password: r.password, name: r.name, geo: r.geo, sees: r.sees, buyer_id: r.buyer_id };
    });
    BUYERS = newBuyers;
    console.log(`[BUYERS] cache refreshed: ${rows.length} buyers`);
  } catch (err) {
    console.error('[BUYERS] cache refresh error:', err.message);
  }
}

const SUPER_LOGIN    = process.env.SUPER_LOGIN    || 'admin';
const SUPER_PASSWORD = process.env.SUPER_PASSWORD || 'supersecret2025';

// ── Telegram ─────────────────────────────────────────────
const TG_BOT_TOKEN = process.env.TG_BOT_TOKEN || '8601777567:AAFyBTaF_uM65ueCJvM4YHCZfu8_7Q08Ezg';
const TG_CHAT_ID   = process.env.TG_CHAT_ID   || '-1003578369883';

// ── Facebook CAPI — динамический кеш из БД ──────────────
let FB_PIXELS = {}; // pixel_id -> access_token (кеш, обновляется из БД)
let FB_PIXELS_META = {}; // pixel_id -> { buyer, label, test_code }

async function refreshPixelCache() {
  try {
    const { rows } = await pool.query(`SELECT pixel_id, access_token, buyer, label, test_code FROM pixels`);
    const newPixels = {};
    const newMeta = {};
    rows.forEach(r => {
      newPixels[r.pixel_id] = r.access_token;
      newMeta[r.pixel_id] = { buyer: r.buyer, label: r.label, test_code: r.test_code || '' };
    });
    FB_PIXELS = newPixels;
    FB_PIXELS_META = newMeta;
    console.log(`[PIXELS] cache refreshed: ${rows.length} pixels`);
  } catch (err) {
    console.error('[PIXELS] cache refresh error:', err.message);
  }
}


// ── HTTPS helpers ─────────────────────────────────────────
function httpsPost(hostname, path, body) {
  return new Promise((resolve) => {
    const bodyStr = JSON.stringify(body);
    const options = {
      hostname, path, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', (e) => resolve({ status: 0, body: e.message }));
    req.write(bodyStr);
    req.end();
  });
}

function httpsGet(url) {
  return new Promise((resolve) => {
    const req = https.get(url, { headers: { 'User-Agent': 'nodejs' } }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { resolve({}); } });
    });
    req.on('error', () => resolve({}));
    req.setTimeout(3000, () => { req.destroy(); resolve({}); });
  });
}

// ── Geo ───────────────────────────────────────────────────
async function getGeo(ip) {
  if (!ip || ip === '127.0.0.1' || ip.startsWith('::')) return {};
  return httpsGet(`https://ipapi.co/${ip}/json/`);
}

// ── TG send with buttons ──────────────────────────────────
function sendLeadToTelegram(lead) {
  const u = lead.utms || {};
  const leadDate = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Kiev' });
  const utmLines = [
    u.utm_source   ? `| source: ${u.utm_source}`       : null,
    u.utm_medium   ? `| medium: ${u.utm_medium}`       : null,
    u.utm_campaign ? `| campaign: ${u.utm_campaign}`   : null,
    u.utm_content  ? `| content: ${u.utm_content}`     : null,
    u.campaign_id  ? `| campaign_id: ${u.campaign_id}` : null,
    u.adset_id     ? `| adset_id: ${u.adset_id}`       : null,
    u.creative_id  ? `| creative_id: ${u.creative_id}` : null,
  ].filter(Boolean).join('\n') || '| -';

  const text = [
    `НОВЫЙ ЛИД — GPT Trade`,
    `ID: ${lead.id.slice(0,8).toUpperCase()}`,
    ``,
    `КОНТАКТЫ`,
    `Имя: ${lead.name}`,
    `Email: ${lead.email}`,
    `Телефон: ${lead.phone}`,
    ``,
    `БАЙЕР: ${lead.buyer}`,
    ``,
    `ГЕО: ${lead.flag || ''} ${lead.country || '-'}${lead.city ? ', ' + lead.city : ''}`,
    ``,
    `UTM`,
    utmLines,
    u.fbclid ? `fbclid: ${u.fbclid.slice(0,24)}...` : null,
    ``,
    `IP: ${lead.ip}`,
    `${leadDate}`,
  ].filter(l => l !== null).join('\n');

  const body = {
    chat_id: TG_CHAT_ID,
    text,
    reply_markup: {
      inline_keyboard: [
        [
          { text: '✅ Апрув', callback_data: `approve_${lead.id}` },
          { text: '❌ Отклонить', callback_data: `reject_${lead.id}` }
        ],
        [
          { text: '💰 Purchase', callback_data: `purchase_${lead.id}` }
        ]
      ]
    }
  };

  return httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/sendMessage`, body)
    .then(r => console.log('[TG]', r.status, r.body.slice(0, 120)));
}

// ── FB CAPI ───────────────────────────────────────────────
async function sendFBEvent(lead) {
  const pixelId = lead.utms && lead.utms.pixel_id;
  if (!pixelId) { console.log('[FB CAPI] no pixel_id, skipping'); return; }
  const accessToken = FB_PIXELS[pixelId];
  if (!accessToken) { console.log('[FB CAPI] no token for pixel', pixelId, '- skipping'); return; }
  console.log(`[FB CAPI] using pixel ${pixelId}`);
  const eventTime = Math.floor(Date.now() / 1000);
  const hash = (val) => val ? crypto.createHash('sha256').update(val.trim().toLowerCase()).digest('hex') : undefined;
  const userData = {};
  if (lead.email) userData.em = [hash(lead.email)];
  if (lead.phone) userData.ph = [hash(lead.phone.replace(/\D/g,''))];
  if (lead.ip)    userData.client_ip_address = lead.ip;
  if (lead.utms && lead.utms.fbclid) {
    // fbc: timestamp ближе к клику (created_at лида), а не к апруву
    const clickTime = lead.created_at ? Math.floor(new Date(lead.created_at).getTime() / 1000) : eventTime;
    userData.fbc = `fb.1.${clickTime}.${lead.utms.fbclid}`;
  }
  if (lead.id) userData.external_id = [hash(lead.id)];
  const eventId = `lead_${lead.id}_${eventTime}`;
  const payload = {
    data: [{
      event_name: 'Lead',
      event_time: eventTime,
      event_id: eventId,
      action_source: 'website',
      event_source_url: (lead.utms && lead.utms.landing) || '',
      user_data: userData,
    }]
  };
  const pixelTestCode = (FB_PIXELS_META[pixelId] && FB_PIXELS_META[pixelId].test_code) || '';
  if (pixelTestCode) payload.test_event_code = pixelTestCode;
  const result = await httpsPost('graph.facebook.com', `/v21.0/${pixelId}/events?access_token=${accessToken}`, payload);
  console.log(`[FB CAPI] pixel=${pixelId} test=${pixelTestCode||'OFF'}`, result.status, result.body.slice(0, 150));
  return result;
}

// ── FB CAPI Purchase (общая функция для TG и CRM) ────────
async function sendFBPurchase(lead) {
  const pixelId = lead.utms && lead.utms.pixel_id;
  if (!pixelId) return null;
  const accessToken = FB_PIXELS[pixelId];
  if (!accessToken) return null;
  const eventTime = Math.floor(Date.now() / 1000);
  const hash = (val) => val ? crypto.createHash('sha256').update(val.trim().toLowerCase()).digest('hex') : undefined;
  const userData = {};
  if (lead.email) userData.em = [hash(lead.email)];
  if (lead.phone) userData.ph = [hash(lead.phone.replace(/\D/g,''))];
  if (lead.ip)    userData.client_ip_address = lead.ip;
  if (lead.id)    userData.external_id = [hash(lead.id)];
  const eventId = `purchase_${lead.id}_${eventTime}`;
  const payload = {
    data: [{
      event_name: 'Purchase',
      event_time: eventTime,
      event_id: eventId,
      action_source: 'website',
      event_source_url: (lead.utms && lead.utms.landing) || '',
      user_data: userData,
      custom_data: { currency: 'USD', value: 500 }
    }]
  };
  const pixelTestCode = (FB_PIXELS_META[pixelId] && FB_PIXELS_META[pixelId].test_code) || '';
  if (pixelTestCode) payload.test_event_code = pixelTestCode;
  const result = await httpsPost('graph.facebook.com', `/v21.0/${pixelId}/events?access_token=${accessToken}`, payload);
  console.log(`[FB CAPI Purchase] pixel=${pixelId} test=${pixelTestCode||'OFF'}`, result.status, result.body.slice(0, 150));
  return result;
}

// ── Auth ──────────────────────────────────────────────────
function authenticateBuyer(token) {
  if (!token) return null;
  const [login, ...rest] = token.split(':');
  const password = rest.join(':');
  if (login === SUPER_LOGIN && password === SUPER_PASSWORD)
    return { login, name: 'Admin', isSuper: true };
  const buyer = BUYERS[login];
  if (!buyer || buyer.password !== password) return null;
  return { login, name: buyer.name, geo: buyer.geo, sees: buyer.sees, isSuper: false };
}

function authMiddleware(req, res, next) {
  const authHeader = req.headers['authorization'] || '';
  const bearerToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
  const token = req.headers['x-auth-token'] || bearerToken || req.query.token || req.query.auth || '';
  const buyer = authenticateBuyer(token);
  if (!buyer) return res.status(401).json({ error: 'Unauthorized' });
  req.buyer = buyer;
  next();
}

// ── Express ───────────────────────────────────────────────
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Auth-Token, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// Обёртка для async route handlers (Express 4 не ловит rejected promises)
const asyncWrap = fn => (req, res, next) => fn(req, res, next).catch(next);

// ── Routes ────────────────────────────────────────────────

// Публичный маппинг buyer_id → buyer name (для фронта)
app.get('/buyer-map', (req, res) => {
  const map = {};
  Object.values(BUYERS).forEach(b => {
    if (b.buyer_id) map[b.buyer_id] = b.sees;
  });
  res.json(map);
});

app.post('/lead', async (req, res) => {
  try {
    const { name, email, phone, buyer: buyerField, utms } = req.body;
    if (!name && !email && !phone) return res.status(400).json({ error: 'Empty lead' });

    const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '';
    const geo = await getGeo(ip);

    const lead = {
      id: crypto.randomUUID(),
      name:  name  || '',
      email: email || '',
      phone: phone || '',
      buyer: buyerField || 'unknown',
      ip,
      ua: req.headers['user-agent'] || '',
      country: geo.country_name || '',
      city:    geo.city || '',
      flag:    geo.country || '',
      utms: utms || {},
      status: 'pending',
    };

    await pool.query(
      `INSERT INTO leads (id,name,email,phone,buyer,ip,ua,country,city,flag,utms,status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [lead.id, lead.name, lead.email, lead.phone, lead.buyer, lead.ip, lead.ua,
       lead.country, lead.city, lead.flag, JSON.stringify(lead.utms), lead.status]
    );

    console.log(`[LEAD] ${lead.name} | ${lead.email} | ${lead.phone} | ${lead.buyer} | ${lead.country}`);
    sendLeadToTelegram(lead);

    res.json({ ok: true, id: lead.id });
  } catch (err) {
    console.error('[LEAD ERROR]', err.message);
    res.status(500).json({ ok: false, error: 'Internal error' });
  }
});

app.post('/visit', async (req, res) => {
  try {
    await pool.query(`UPDATE visits SET count = count + 1 WHERE id = 1`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[VISIT ERROR]', err.message);
    res.status(500).json({ ok: false });
  }
});

app.post('/tg-webhook', async (req, res) => {
  res.sendStatus(200);
  try {
  const cb = req.body && req.body.callback_query;
  if (!cb) return;
  const data_str = cb.data || '';
  const msgId  = cb.message && cb.message.message_id;
  const chatId = cb.message && cb.message.chat && cb.message.chat.id;
  const cbId   = cb.id;

  // ── Approve / Reject ──
  if (data_str.startsWith('approve_') || data_str.startsWith('reject_')) {
    const isApprove = data_str.startsWith('approve_');
    const leadId = data_str.replace(/^(approve|reject)_/, '');
    const newStatus = isApprove ? 'approved' : 'rejected';

    await pool.query(`UPDATE leads SET status=$1 WHERE id=$2`, [newStatus, leadId]);

    if (isApprove) {
      const { rows } = await pool.query(`SELECT * FROM leads WHERE id=$1`, [leadId]);
      if (rows[0]) {
        rows[0].utms = typeof rows[0].utms === 'string' ? JSON.parse(rows[0].utms) : (rows[0].utms || {});
        console.log('[APPROVE] pixel_id from DB:', rows[0].utms.pixel_id, '| buyer:', rows[0].buyer);
        await sendFBEvent(rows[0]);
      }
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
        { callback_query_id: cbId, text: 'Апрув отправлен в Facebook!' });
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`,
        { chat_id: chatId, message_id: msgId, reply_markup: { inline_keyboard: [
          [{ text: '✅ АПРУВНУТ', callback_data: 'done' }],
          [{ text: '💰 Purchase', callback_data: `purchase_${leadId}` }]
        ]}});
    } else {
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
        { callback_query_id: cbId, text: 'Лид отклонён' });
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`,
        { chat_id: chatId, message_id: msgId, reply_markup: { inline_keyboard: [[{ text: '❌ ОТКЛОНЁН', callback_data: 'done' }]] } });
    }
    return;
  }

  // ── Purchase — шаг 2: подтверждение ──
  if (data_str.startsWith('purchase_confirm_')) {
    const leadId = data_str.replace('purchase_confirm_', '');
    const { rows } = await pool.query(`SELECT * FROM leads WHERE id=$1`, [leadId]);
    if (!rows[0]) return;
    // Защита от двойного Purchase
    if (rows[0].status === 'purchased') {
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
        { callback_query_id: cbId, text: '⚠️ Purchase уже отправлен ранее' });
      return;
    }
    rows[0].utms = rows[0].utms || {};
    const result = await sendFBPurchase(rows[0]);
    if (!result) {
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
        { callback_query_id: cbId, text: 'Ошибка: нет pixel_id или токена' });
      return;
    }
    await pool.query(`UPDATE leads SET status='purchased' WHERE id=$1`, [leadId]);
    await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
      { callback_query_id: cbId, text: '💰 Purchase отправлен в Facebook!' });
    await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`,
      { chat_id: chatId, message_id: msgId, reply_markup: { inline_keyboard: [[{ text: '💰 PURCHASE ОТПРАВЛЕН', callback_data: 'done' }]] } });
    return;
  }

  // ── Purchase — отмена ──
  if (data_str.startsWith('purchase_cancel_')) {
    await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
      { callback_query_id: cbId, text: 'Отменено' });
    await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`,
      { chat_id: chatId, message_id: msgId, reply_markup: { inline_keyboard: [[{ text: '❌ Отменено', callback_data: 'done' }]] } });
    return;
  }

  // ── Purchase — шаг 1: запрос подтверждения ──
  if (data_str.startsWith('purchase_')) {
    const leadId = data_str.replace('purchase_', '');
    await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
      { callback_query_id: cbId, text: 'Подтвердите отправку Purchase' });
    await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/sendMessage`, {
      chat_id: chatId,
      text: `💰 Отправить событие Purchase для лида ${leadId.slice(0,8).toUpperCase()}?`,
      reply_markup: { inline_keyboard: [[
        { text: '✅ Да, отправить', callback_data: `purchase_confirm_${leadId}` },
        { text: '❌ Отмена', callback_data: `purchase_cancel_${leadId}` }
      ]]}
    });
    return;
  }

  } catch (err) {
    console.error('[TG WEBHOOK ERROR]', err.message);
  }
});

app.post('/admin/auth', (req, res) => {
  const { login, password } = req.body;
  const buyer = authenticateBuyer(`${login}:${password}`);
  if (!buyer) return res.status(401).json({ ok: false, error: 'Invalid credentials' });
  res.json({ ok: true, token: `${login}:${password}`, name: buyer.name, isSuper: buyer.isSuper });
});

app.get('/admin/stats', authMiddleware, asyncWrap(async (req, res) => {
  const sees = (!req.buyer.isSuper) ? (BUYERS[req.buyer.login]?.sees || req.buyer.name) : null;
  const whereClause = sees ? `WHERE buyer=$1` : '';
  const params = sees ? [sees] : [];

  const totalRes  = await pool.query(`SELECT COUNT(*) FROM leads ${whereClause}`, params);
  const todayRes  = await pool.query(`SELECT COUNT(*) FROM leads ${whereClause ? whereClause + ' AND' : 'WHERE'} created_at::date = CURRENT_DATE`, params);
  const visitsRes = await pool.query(`SELECT count FROM visits WHERE id=1`);

  // 7 дней одним запросом вместо 7 отдельных
  const byDayRes = await pool.query(
    `SELECT created_at::date AS day, COUNT(*) AS count FROM leads
     ${whereClause ? whereClause + ' AND' : 'WHERE'} created_at >= CURRENT_DATE - INTERVAL '6 days'
     GROUP BY created_at::date ORDER BY day`, params
  );
  const byDay = {};
  for (let i = 6; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    byDay[d.toISOString().slice(0,10)] = 0;
  }
  byDayRes.rows.forEach(r => { byDay[r.day.toISOString().slice(0,10)] = parseInt(r.count); });

  const total  = parseInt(totalRes.rows[0].count);
  const visits = parseInt(visitsRes.rows[0]?.count || 0);
  const cr = visits > 0 ? ((total / visits) * 100).toFixed(1) : '0.0';

  res.json({ total, today: parseInt(todayRes.rows[0].count), visits, cr, byDay });
}));

app.get('/admin/leads', authMiddleware, asyncWrap(async (req, res) => {
  const sees = (!req.buyer.isSuper) ? (BUYERS[req.buyer.login]?.sees || req.buyer.name) : null;
  const { search = '', page = 1, limit = 20 } = req.query;
  const offset = (parseInt(page) - 1) * parseInt(limit);

  let where = sees ? `WHERE buyer=$1` : `WHERE TRUE`;
  let params = sees ? [sees] : [];

  if (search) {
    const idx = params.length + 1;
    where += ` AND (name ILIKE $${idx} OR email ILIKE $${idx} OR phone ILIKE $${idx})`;
    params.push(`%${search}%`);
  }

  const countRes = await pool.query(`SELECT COUNT(*) FROM leads ${where}`, params);
  const leadsRes = await pool.query(
    `SELECT * FROM leads ${where} ORDER BY created_at DESC LIMIT $${params.length+1} OFFSET $${params.length+2}`,
    [...params, parseInt(limit), offset]
  );

  const total = parseInt(countRes.rows[0].count);
  const lim = parseInt(limit);
  const pg = parseInt(page);
  const pages = Math.ceil(total / lim) || 1;
  res.json({ total, leads: leadsRes.rows, page: pg, pages });
}));

app.post('/admin/purchase/:id', authMiddleware, asyncWrap(async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM leads WHERE id=$1`, [req.params.id]);
  if (!rows[0]) return res.status(404).json({ ok: false, error: 'Lead not found' });
  const lead = rows[0];
  if (lead.status === 'purchased') return res.status(400).json({ ok: false, error: 'Purchase уже отправлен ранее' });
  lead.utms = lead.utms || {};
  const result = await sendFBPurchase(lead);
  if (!result) return res.status(400).json({ ok: false, error: 'No pixel_id or token for this lead' });
  if (result.status === 200) {
    await pool.query(`UPDATE leads SET status='purchased' WHERE id=$1`, [req.params.id]);
    res.json({ ok: true });
  } else {
    res.status(400).json({ ok: false, error: result.body });
  }
}));

app.delete('/admin/lead/:id', authMiddleware, asyncWrap(async (req, res) => {
  await pool.query(`DELETE FROM leads WHERE id=$1`, [req.params.id]);
  res.json({ ok: true });
}));

app.get('/admin/export.csv', authMiddleware, asyncWrap(async (req, res) => {
  const sees = (!req.buyer.isSuper) ? (BUYERS[req.buyer.login]?.sees || req.buyer.name) : null;
  const where = sees ? `WHERE buyer=$1` : '';
  const params = sees ? [sees] : [];
  const { rows } = await pool.query(`SELECT * FROM leads ${where} ORDER BY created_at DESC`, params);

  const csv = [
    ['ID','Name','Email','Phone','Buyer','Country','City','Status','IP','Date','utm_source','utm_campaign'],
    ...rows.map(l => [
      l.id, l.name, l.email, l.phone, l.buyer,
      l.country||'', l.city||'', l.status||'', l.ip,
      l.created_at, l.utms?.utm_source||'', l.utms?.utm_campaign||''
    ])
  ].map(r => r.map(v => `"${String(v||'').replace(/"/g,'""')}"`).join(',')).join('\n');

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="leads.csv"');
  res.send(csv);
}));

// ── Pixels CRUD ──────────────────────────────────────────

// Список пикселей (байер видит свои, админ — все)
app.get('/admin/pixels', authMiddleware, asyncWrap(async (req, res) => {
  const sees = (!req.buyer.isSuper) ? (BUYERS[req.buyer.login]?.sees || req.buyer.name) : null;
  const where = sees ? `WHERE buyer=$1` : '';
  const params = sees ? [sees] : [];
  const { rows } = await pool.query(`SELECT pixel_id, buyer, label, test_code, created_at FROM pixels ${where} ORDER BY created_at DESC`, params);
  res.json({ pixels: rows });
}));

// Добавить пиксель
app.post('/admin/pixels', authMiddleware, async (req, res) => {
  const { pixel_id, access_token, label, test_code } = req.body;
  if (!pixel_id || !access_token) return res.status(400).json({ ok: false, error: 'pixel_id и access_token обязательны' });
  const buyer = req.buyer.isSuper ? (req.body.buyer || req.buyer.name) : (BUYERS[req.buyer.login]?.sees || req.buyer.name);
  try {
    await pool.query(
      `INSERT INTO pixels (pixel_id, access_token, buyer, label, test_code) VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (pixel_id) DO UPDATE SET access_token=$2, buyer=$3, label=$4, test_code=$5`,
      [pixel_id.trim(), access_token.trim(), buyer, label || '', test_code || '']
    );
    await refreshPixelCache();
    res.json({ ok: true });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

// Обновить токен пикселя
app.put('/admin/pixels/:pixel_id', authMiddleware, asyncWrap(async (req, res) => {
  const { access_token, label, buyer: newBuyer, test_code } = req.body;
  const pid = req.params.pixel_id;
  if (!req.buyer.isSuper) {
    const sees = BUYERS[req.buyer.login]?.sees || req.buyer.name;
    const { rows } = await pool.query(`SELECT buyer FROM pixels WHERE pixel_id=$1`, [pid]);
    if (!rows[0] || rows[0].buyer !== sees) return res.status(403).json({ ok: false, error: 'Нет доступа' });
  }
  const updates = [];
  const params = [];
  let idx = 1;
  if (access_token) { updates.push(`access_token=$${idx++}`); params.push(access_token.trim()); }
  if (label !== undefined) { updates.push(`label=$${idx++}`); params.push(label); }
  if (newBuyer && req.buyer.isSuper) { updates.push(`buyer=$${idx++}`); params.push(newBuyer); }
  if (test_code !== undefined) { updates.push(`test_code=$${idx++}`); params.push(test_code); }
  if (!updates.length) return res.status(400).json({ ok: false, error: 'Нечего обновлять' });
  params.push(pid);
  await pool.query(`UPDATE pixels SET ${updates.join(',')} WHERE pixel_id=$${idx}`, params);
  await refreshPixelCache();
  res.json({ ok: true });
}));

// Удалить пиксель
app.delete('/admin/pixels/:pixel_id', authMiddleware, asyncWrap(async (req, res) => {
  const pid = req.params.pixel_id;
  if (!req.buyer.isSuper) {
    const sees = BUYERS[req.buyer.login]?.sees || req.buyer.name;
    const { rows } = await pool.query(`SELECT buyer FROM pixels WHERE pixel_id=$1`, [pid]);
    if (!rows[0] || rows[0].buyer !== sees) return res.status(403).json({ ok: false, error: 'Нет доступа' });
  }
  await pool.query(`DELETE FROM pixels WHERE pixel_id=$1`, [pid]);
  await refreshPixelCache();
  res.json({ ok: true });
}));

// Тест CAPI пикселя — отправляет тестовое событие
app.post('/admin/pixels/:pixel_id/test', authMiddleware, asyncWrap(async (req, res) => {
  const pid = req.params.pixel_id;
  const token = FB_PIXELS[pid];
  if (!token) return res.status(404).json({ ok: false, error: 'Пиксель не найден' });
  const eventTime = Math.floor(Date.now() / 1000);
  const payload = {
    data: [{
      event_name: 'Lead',
      event_time: eventTime,
      event_id: `test_${eventTime}`,
      action_source: 'website',
      user_data: { em: [crypto.createHash('sha256').update('test@test.com').digest('hex')] },
    }],
    test_event_code: req.body.test_code || 'TEST00000'
  };
  const result = await httpsPost('graph.facebook.com', `/v21.0/${pid}/events?access_token=${token}`, payload);
  let body;
  try { body = JSON.parse(result.body); } catch(e) { body = result.body; }
  res.json({ ok: result.status === 200, status: result.status, response: body });
}));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));

// ── Buyers CRUD (только admin) ──────────────────────────

function requireSuper(req, res, next) {
  if (!req.buyer.isSuper) return res.status(403).json({ ok: false, error: 'Только admin' });
  next();
}

// Список байеров
app.get('/admin/buyers', authMiddleware, requireSuper, asyncWrap(async (req, res) => {
  const { rows } = await pool.query(`
    SELECT b.login, b.name, b.geo, b.sees, b.buyer_id, b.created_at,
      COALESCE(px.cnt, 0)::int AS pixels,
      COALESCE(ld.cnt, 0)::int AS leads
    FROM buyers b
    LEFT JOIN (SELECT buyer, COUNT(*) AS cnt FROM pixels GROUP BY buyer) px ON px.buyer = b.sees
    LEFT JOIN (SELECT buyer, COUNT(*) AS cnt FROM leads GROUP BY buyer) ld ON ld.buyer = b.sees
    ORDER BY b.created_at
  `);
  res.json({ buyers: rows });
}));

// Добавить байера
app.post('/admin/buyers', authMiddleware, requireSuper, asyncWrap(async (req, res) => {
  const { login, password, name, geo, sees } = req.body;
  if (!login || !password || !name) return res.status(400).json({ ok: false, error: 'login, password и name обязательны' });
  if (login === SUPER_LOGIN) return res.status(400).json({ ok: false, error: 'Этот логин зарезервирован' });
  try {
    await pool.query(
      `INSERT INTO buyers (login, password, name, geo, sees) VALUES ($1,$2,$3,$4,$5)`,
      [login.trim(), password, name.trim(), geo || '', (sees || name).trim()]
    );
    await refreshBuyersCache();
    res.json({ ok: true });
  } catch (err) {
    if (err.code === '23505') return res.status(400).json({ ok: false, error: 'Логин уже существует' });
    res.status(400).json({ ok: false, error: err.message });
  }
}));

// Обновить байера
app.put('/admin/buyers/:login', authMiddleware, requireSuper, asyncWrap(async (req, res) => {
  const { password, name, geo, sees } = req.body;
  const buyerLogin = req.params.login;
  const updates = [];
  const params = [];
  let idx = 1;
  if (password) { updates.push(`password=$${idx++}`); params.push(password); }
  if (name) { updates.push(`name=$${idx++}`); params.push(name.trim()); }
  if (geo !== undefined) { updates.push(`geo=$${idx++}`); params.push(geo); }
  if (sees) { updates.push(`sees=$${idx++}`); params.push(sees.trim()); }
  if (!updates.length) return res.status(400).json({ ok: false, error: 'Нечего обновлять' });
  params.push(buyerLogin);
  const result = await pool.query(`UPDATE buyers SET ${updates.join(',')} WHERE login=$${idx}`, params);
  if (result.rowCount === 0) return res.status(404).json({ ok: false, error: 'Байер не найден' });
  await refreshBuyersCache();
  res.json({ ok: true });
}));

// Удалить байера
app.delete('/admin/buyers/:login', authMiddleware, requireSuper, asyncWrap(async (req, res) => {
  const buyerLogin = req.params.login;
  const result = await pool.query(`DELETE FROM buyers WHERE login=$1`, [buyerLogin]);
  if (result.rowCount === 0) return res.status(404).json({ ok: false, error: 'Байер не найден' });
  await refreshBuyersCache();
  res.json({ ok: true });
}));

// ── Global Express error handler ─────────────────────────
app.use((err, req, res, next) => {
  console.error('[EXPRESS ERROR]', req.method, req.path, err.message);
  if (!res.headersSent) res.status(500).json({ ok: false, error: 'Internal server error' });
});

// ── Start ─────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await initDB();
  // TG webhook
  await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/deleteWebhook`, {});
  const r = await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/setWebhook`,
    { url: 'https://backend-land.onrender.com/tg-webhook', allowed_updates: ['callback_query'] });
  console.log('[TG webhook]', r.status, r.body.slice(0, 100));
});

// ── Global error handlers ────────────────────────────────
pool.on('error', (err) => console.error('[PG pool error]', err.message));
process.on('unhandledRejection', (err) => console.error('[Unhandled rejection]', err));
