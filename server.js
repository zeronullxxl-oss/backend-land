const express = require('express');
const fs      = require('fs');
const path    = require('path');
const crypto  = require('crypto');
const https   = require('https');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Байеры ──────────────────────────────────────────────
const DEFAULT_BUYERS = {
  'gpttrade': { password: 'pass123', name: 'pumba',    geo: 'MX', sees: 'pumba'    },
  'nepravda': { password: 'pass456', name: 'nepravda', geo: 'MX', sees: 'nepravda' },
};
const BUYERS = process.env.BUYERS_JSON
  ? JSON.parse(process.env.BUYERS_JSON)
  : DEFAULT_BUYERS;

const SUPER_LOGIN    = process.env.SUPER_LOGIN    || 'admin';
const SUPER_PASSWORD = process.env.SUPER_PASSWORD || 'supersecret2025';

// ── Telegram ─────────────────────────────────────────────
const TG_BOT_TOKEN = process.env.TG_BOT_TOKEN || '8601777567:AAFyBTaF_uM65ueCJvM4YHCZfu8_7Q08Ezg';
const TG_CHAT_ID   = process.env.TG_CHAT_ID   || '-1003578369883';

// ── Facebook CAPI ─────────────────────────────────────────
const FB_PIXEL_ID    = process.env.FB_PIXEL_ID    || '952067224177021';
const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN || 'EAFs8v6OUISMBQ0tBJZA9uKoungB65OARvYO9f3V1e61GmLyOVk8RmmiZAvbO8yh9SkTzU5EZB5lPDEAfJgxmjuZCwkpMsZA4n66xnaYrttGK2BtCzej77aabrvV3ZBemBpE7PLFgRZC8hpdZA07OKq8C6pd4V7STfW9khM12RCav8mD22zcQR2Kmvr68cBiNCwZDZD';

// ── HTTPS helper ──────────────────────────────────────────
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

// ── Telegram send with approve buttons ───────────────────
function sendLeadToTelegram(lead) {
  const u = lead.utms || {};
  const leadDate = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Kiev' });
  const utmLines = [
    u.utm_source   ? `├ source: ${u.utm_source}`       : null,
    u.utm_medium   ? `├ medium: ${u.utm_medium}`       : null,
    u.utm_campaign ? `├ campaign: ${u.utm_campaign}`   : null,
    u.utm_content  ? `├ content: ${u.utm_content}`     : null,
    u.campaign_id  ? `├ campaign_id: ${u.campaign_id}` : null,
    u.adset_id     ? `├ adset_id: ${u.adset_id}`       : null,
    u.creative_id  ? `└ creative_id: ${u.creative_id}` : null,
  ].filter(Boolean).join('\n') || '└ -';

  const text = [
    `🔔 *НОВЫЙ ЛИД — GPT Trade*`,
    `📋 ID: \`${lead.id.slice(0,8).toUpperCase()}\``,
    ``,
    `👤 *КОНТАКТЫ*`,
    `├ Имя: ${lead.name}`,
    `├ Email: ${lead.email}`,
    `└ Телефон: ${lead.phone}`,
    ``,
    `👷 *БАЙЕР*`,
    `└ ${lead.buyer}`,
    ``,
    `🌍 *ГЕО*`,
    `└ ${lead.flag || ''} ${lead.country || '-'}${lead.city ? ', ' + lead.city : ''}`,
    ``,
    `📊 *UTM*`,
    utmLines,
    u.fbclid ? `🔗 fbclid: ${u.fbclid.slice(0,24)}...` : null,
    ``,
    `📍 IP: ${lead.ip}`,
    `⏰ ${leadDate}`,
  ].filter(l => l !== null).join('\n');

  const body = {
    chat_id: TG_CHAT_ID,
    text,
    parse_mode: 'Markdown',
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ Апрув', callback_data: `approve_${lead.id}` },
        { text: '❌ Отклонить', callback_data: `reject_${lead.id}` }
      ]]
    }
  };

  return httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/sendMessage`, body)
    .then(r => console.log('[TG response]', r.status, r.body.slice(0, 100)));
}

// ── Facebook CAPI event ───────────────────────────────────
async function sendFBEvent(lead) {
  const pixelId = (lead.utms && lead.utms.pixel_id) || FB_PIXEL_ID;
  const eventTime = Math.floor(Date.now() / 1000);

  // Hash helpers
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
    }]
  };

  const result = await httpsPost(
    'graph.facebook.com',
    `/v19.0/${pixelId}/events?access_token=${FB_ACCESS_TOKEN}`,
    payload
  );
  console.log('[FB CAPI]', result.status, result.body.slice(0, 150));
  return result;
}

// ── Geo lookup ───────────────────────────────────────────
async function getGeo(ip) {
  if (!ip || ip === '127.0.0.1' || ip.startsWith('::')) return {};
  return httpsGet(`https://ipapi.co/${ip}/json/`);
}

// ── Storage ───────────────────────────────────────────────
const DATA_FILE = path.join(__dirname, 'leads.json');

function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch(e) {}
  return { leads: [], visits: 0 };
}

function saveData(data) {
  try { fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2)); } catch(e) {}
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

// ── Express setup ─────────────────────────────────────────
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Auth-Token, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ── Routes ────────────────────────────────────────────────

// POST /lead
app.post('/lead', async (req, res) => {
  const { name, email, phone, buyer: buyerField, utms } = req.body;
  if (!name && !email && !phone) return res.status(400).json({ error: 'Empty lead' });

  const data = loadData();
  const lead = {
    id: crypto.randomUUID(),
    name:  name  || '',
    email: email || '',
    phone: phone || '',
    buyer: buyerField || 'unknown',
    ip: req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '',
    ua: req.headers['user-agent'] || '',
    utms: utms || {},
    status: 'pending',
    createdAt: new Date().toISOString()
  };

  // Geo
  const geo = await getGeo(lead.ip);
  lead.country = geo.country_name || '';
  lead.city    = geo.city || '';
  lead.flag    = geo.country || '';

  data.leads.unshift(lead);
  saveData(data);
  console.log(`[LEAD] ${lead.name} | ${lead.email} | ${lead.phone} | ${lead.buyer} | ${lead.country}`);

  // TG с кнопками
  sendLeadToTelegram(lead);

  res.json({ ok: true, id: lead.id });
});

// POST /tg-webhook — нажатия кнопок от Telegram
app.post('/tg-webhook', async (req, res) => {
  res.sendStatus(200); // отвечаем сразу

  const cb = req.body && req.body.callback_query;
  if (!cb) return;

  const data_str = cb.data || '';
  const msgId    = cb.message && cb.message.message_id;
  const chatId   = cb.message && cb.message.chat && cb.message.chat.id;
  const cbId     = cb.id;

  // Апрув
  if (data_str.startsWith('approve_')) {
    const leadId = data_str.replace('approve_', '');
    const data = loadData();
    const lead = data.leads.find(l => l.id === leadId || l.id.startsWith(leadId));

    if (lead) {
      lead.status = 'approved';
      saveData(data);

      // FB CAPI
      const fbResult = await sendFBEvent(lead);

      // Ответ в TG
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`, {
        callback_query_id: cbId, text: '✅ Апрув отправлен в Facebook!'
      });
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`, {
        chat_id: chatId, message_id: msgId,
        reply_markup: { inline_keyboard: [[{ text: '✅ АПРУВНУТ', callback_data: 'done' }]] }
      });
    }
  }

  // Отклонить
  if (data_str.startsWith('reject_')) {
    const leadId = data_str.replace('reject_', '');
    const data = loadData();
    const lead = data.leads.find(l => l.id === leadId || l.id.startsWith(leadId));

    if (lead) {
      lead.status = 'rejected';
      saveData(data);

      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/answerCallbackQuery`, {
        callback_query_id: cbId, text: '❌ Лид отклонён'
      });
      await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/editMessageReplyMarkup`, {
        chat_id: chatId, message_id: msgId,
        reply_markup: { inline_keyboard: [[{ text: '❌ ОТКЛОНЁН', callback_data: 'done' }]] }
      });
    }
  }
});

// POST /visit
app.post('/visit', (req, res) => {
  const data = loadData();
  data.visits = (data.visits || 0) + 1;
  saveData(data);
  res.json({ ok: true });
});

// POST /admin/auth
app.post('/admin/auth', (req, res) => {
  const { login, password } = req.body;
  const buyer = authenticateBuyer(`${login}:${password}`);
  if (!buyer) return res.status(401).json({ ok: false, error: 'Invalid credentials' });
  res.json({ ok: true, token: `${login}:${password}`, name: buyer.name, isSuper: buyer.isSuper });
});

// GET /admin/stats
app.get('/admin/stats', authMiddleware, (req, res) => {
  let data = loadData();
  let leads = data.leads || [];
  if (!req.buyer.isSuper) {
    const sees = req.buyer.sees || req.buyer.name;
    leads = leads.filter(l => l.buyer === sees);
  }
  const today = new Date().toISOString().slice(0, 10);
  const todayCount = leads.filter(l => l.createdAt?.startsWith(today)).length;
  const days = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    const key = d.toISOString().slice(0, 10);
    days.push({ date: key, count: leads.filter(l => l.createdAt?.startsWith(key)).length });
  }
  res.json({ total: leads.length, today: todayCount, visits: data.visits || 0, days });
});

// GET /admin/leads
app.get('/admin/leads', authMiddleware, (req, res) => {
  let data = loadData();
  let leads = data.leads || [];
  if (!req.buyer.isSuper) {
    const sees = req.buyer.sees || req.buyer.name;
    leads = leads.filter(l => l.buyer === sees);
  }
  const { search = '', page = 1, limit = 20 } = req.query;
  if (search) {
    const q = search.toLowerCase();
    leads = leads.filter(l =>
      l.name?.toLowerCase().includes(q) ||
      l.email?.toLowerCase().includes(q) ||
      l.phone?.toLowerCase().includes(q)
    );
  }
  const total = leads.length;
  const start = (parseInt(page) - 1) * parseInt(limit);
  leads = leads.slice(start, start + parseInt(limit));
  res.json({ total, leads });
});

// DELETE /admin/lead/:id
app.delete('/admin/lead/:id', authMiddleware, (req, res) => {
  const data = loadData();
  const before = data.leads.length;
  data.leads = data.leads.filter(l => l.id !== req.params.id);
  saveData(data);
  res.json({ ok: true, deleted: before - data.leads.length });
});

// GET /admin/export.csv
app.get('/admin/export.csv', authMiddleware, (req, res) => {
  let data = loadData();
  let leads = data.leads || [];
  if (!req.buyer.isSuper) {
    const sees = req.buyer.sees || req.buyer.name;
    leads = leads.filter(l => l.buyer === sees);
  }
  const rows = [
    ['ID','Name','Email','Phone','Buyer','Country','City','Status','IP','Date','utm_source','utm_campaign'],
    ...leads.map(l => [
      l.id, l.name, l.email, l.phone, l.buyer,
      l.country||'', l.city||'', l.status||'',
      l.ip, l.createdAt,
      l.utms?.utm_source||'', l.utms?.utm_campaign||'',
    ])
  ].map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(','));
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="leads.csv"');
  res.send(rows.join('\n'));
});

// Static
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));

// ── Start + TG webhook setup ──────────────────────────────
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  // Регистрируем webhook в Telegram
  const webhookUrl = `https://backend-land.onrender.com/tg-webhook`;
  const r = await httpsPost('api.telegram.org', `/bot${TG_BOT_TOKEN}/setWebhook`, { url: webhookUrl });
  console.log('[TG webhook]', r.status, r.body.slice(0, 100));
});
