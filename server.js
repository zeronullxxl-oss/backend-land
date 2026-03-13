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
  `);
  console.log('[DB] initialized');
}

// ── Байеры ──────────────────────────────────────────────
const DEFAULT_BUYERS = {
  'gpttrade': { password: 'pass123', name: 'pumba',    geo: 'MX', sees: 'pumba'    },
  'nepravda': { password: 'pass456', name: 'nepravda', geo: 'MX', sees: 'nepravda' },
};
const BUYERS = process.env.BUYERS_JSON ? JSON.parse(process.env.BUYERS_JSON) : DEFAULT_BUYERS;
const SUPER_LOGIN    = process.env.SUPER_LOGIN    || 'admin';
const SUPER_PASSWORD = process.env.SUPER_PASSWORD || 'supersecret2025';

// ── Telegram ─────────────────────────────────────────────
const TG_BOT_TOKEN = process.env.TG_BOT_TOKEN || '8601777567:AAFyBTaF_uM65ueCJvM4YHCZfu8_7Q08Ezg';
const TG_CHAT_ID   = process.env.TG_CHAT_ID   || '-1003578369883';

// ── Facebook CAPI ─────────────────────────────────────────
// Маппинг pixel_id -> access_token
const FB_PIXELS = {
  '952067224177021': 'EAFs8v6OUISMBQ0tBJZA9uKoungB65OARvYO9f3V1e61GmLyOVk8RmmiZAvbO8yh9SkTzU5EZB5lPDEAfJgxmjuZCwkpMsZA4n66xnaYrttGK2BtCzej77aabrvV3ZBemBpE7PLFgRZC8hpdZA07OKq8C6pd4V7STfW9khM12RCav8mD22zcQR2Kmvr68cBiNCwZDZD',
  '711965651910969': 'EAAQyAUopLEUBQxC1ubD9AW4a55I9sMRGDCsMkYMkWXlJOLHuiEGr8LnNqnDCwcfss9V2aD51Q1uFMbwwfZB7l0ZA68n99ZCxTIDZBYTECjRijbI73tSJeA45xVhgTRoCLpUVIuUZA6UyEQa4ZA6KHfLi4CwXyg02P966akKcTiJ8eFn5R4SPJqTFdnaQ75LwZDZD',
};
const FB_DEFAULT_PIXEL = '952067224177021';

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
      inline_keyboard: [[
        { text: '✅ Апрув', callback_data: `approve_${lead.id}` },
        { text: '❌ Отклонить', callback_data: `reject_${lead.id}` }
      ]]
    }
  };

  return httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/sendMessage`, body)
    .then(r => console.log('[TG]', r.status, r.body.slice(0, 120)));
}

// ── FB CAPI ───────────────────────────────────────────────
async function sendFBEvent(lead) {
  const pixelId = (lead.utms && lead.utms.pixel_id) || FB_DEFAULT_PIXEL;
  const accessToken = FB_PIXELS[pixelId];
  if (!accessToken) {
    console.log(`[FB CAPI] unknown pixel_id: ${pixelId}`);
    return;
  }
  console.log(`[FB CAPI] using pixel ${pixelId}`);
  const eventTime = Math.floor(Date.now() / 1000);
  const hash = (val) => val ? crypto.createHash('sha256').update(val.trim().toLowerCase()).digest('hex') : undefined;
  const userData = {};
  if (lead.email) userData.em = [hash(lead.email)];
  if (lead.phone) userData.ph = [hash(lead.phone.replace(/\D/g,''))];
  if (lead.ip)    userData.client_ip_address = lead.ip;
  if (lead.utms && lead.utms.fbclid) userData.fbc = `fb.1.${eventTime}.${lead.utms.fbclid}`;
  const payload = {
    data: [{
      event_name: 'Lead',
      event_time: eventTime,
      action_source: 'website',
      event_source_url: (lead.utms && lead.utms.landing) || '',
      user_data: userData,
    }],
    test_event_code: process.env.FB_TEST_CODE || 'TEST17622'
  };
  const result = await httpsPost('graph.facebook.com', `/v19.0/${pixelId}/events?access_token=${accessToken}`, payload);
  console.log('[FB CAPI]', result.status, result.body.slice(0, 150));
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
  const token = req.headers['x-auth-token'] || bearerToken || req.query.token || '';
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
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ── Routes ────────────────────────────────────────────────

app.post('/lead', async (req, res) => {
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

  // Визит не считаем здесь — только лид
  console.log(`[LEAD] ${lead.name} | ${lead.email} | ${lead.phone} | ${lead.buyer} | ${lead.country}`);
  sendLeadToTelegram(lead);

  res.json({ ok: true, id: lead.id });
});

app.post('/visit', async (req, res) => {
  await pool.query(`UPDATE visits SET count = count + 1 WHERE id = 1`);
  res.json({ ok: true });
});

app.post('/tg-webhook', async (req, res) => {
  res.sendStatus(200);
  const cb = req.body && req.body.callback_query;
  if (!cb) return;
  const data_str = cb.data || '';
  const msgId  = cb.message && cb.message.message_id;
  const chatId = cb.message && cb.message.chat && cb.message.chat.id;
  const cbId   = cb.id;

  if (data_str.startsWith('approve_') || data_str.startsWith('reject_')) {
    const isApprove = data_str.startsWith('approve_');
    const leadId = data_str.replace(/^(approve|reject)_/, '');
    const newStatus = isApprove ? 'approved' : 'rejected';

    await pool.query(`UPDATE leads SET status=$1 WHERE id=$2`, [newStatus, leadId]);

    if (isApprove) {
      const { rows } = await pool.query(`SELECT * FROM leads WHERE id=$1`, [leadId]);
      if (rows[0]) {
        rows[0].utms = rows[0].utms || {};
        await sendFBEvent(rows[0]);
      }
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
        { callback_query_id: cbId, text: 'Апрув отправлен в Facebook!' });
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`,
        { chat_id: chatId, message_id: msgId, reply_markup: { inline_keyboard: [[{ text: 'АПРУВНУТ', callback_data: 'done' }]] } });
    } else {
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`,
        { callback_query_id: cbId, text: 'Лид отклонён' });
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`,
        { chat_id: chatId, message_id: msgId, reply_markup: { inline_keyboard: [[{ text: 'ОТКЛОНЁН', callback_data: 'done' }]] } });
    }
  }
});

app.post('/admin/auth', (req, res) => {
  const { login, password } = req.body;
  const buyer = authenticateBuyer(`${login}:${password}`);
  if (!buyer) return res.status(401).json({ ok: false, error: 'Invalid credentials' });
  res.json({ ok: true, token: `${login}:${password}`, name: buyer.name, isSuper: buyer.isSuper });
});

app.get('/admin/stats', authMiddleware, async (req, res) => {
  const sees = (!req.buyer.isSuper) ? (BUYERS[req.buyer.login]?.sees || req.buyer.name) : null;
  const whereClause = sees ? `WHERE buyer=$1` : '';
  const params = sees ? [sees] : [];

  const totalRes  = await pool.query(`SELECT COUNT(*) FROM leads ${whereClause}`, params);
  const todayRes  = await pool.query(`SELECT COUNT(*) FROM leads ${whereClause ? whereClause + ' AND' : 'WHERE'} created_at::date = CURRENT_DATE`, params);
  const visitsRes = await pool.query(`SELECT count FROM visits WHERE id=1`);

  const days = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    const dateStr = d.toISOString().slice(0,10);
    const dayParams = sees ? [sees, dateStr] : [dateStr];
    const dayWhere  = sees ? `WHERE buyer=$1 AND created_at::date = $2::date` : `WHERE created_at::date = $1::date`;
    const r = await pool.query(`SELECT COUNT(*) FROM leads ${dayWhere}`, dayParams);
    days.push({ date: dateStr, count: parseInt(r.rows[0].count) });
  }

  res.json({
    total:  parseInt(totalRes.rows[0].count),
    today:  parseInt(todayRes.rows[0].count),
    visits: parseInt(visitsRes.rows[0]?.count || 0),
    days
  });
});

app.get('/admin/leads', authMiddleware, async (req, res) => {
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

  res.json({ total: parseInt(countRes.rows[0].count), leads: leadsRes.rows });
});

app.delete('/admin/lead/:id', authMiddleware, async (req, res) => {
  await pool.query(`DELETE FROM leads WHERE id=$1`, [req.params.id]);
  res.json({ ok: true });
});

app.get('/admin/export.csv', authMiddleware, async (req, res) => {
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
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));

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
