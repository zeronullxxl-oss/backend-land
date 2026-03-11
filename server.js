const express = require('express');
const fs      = require('fs');
const path    = require('path');
const crypto  = require('crypto');
const https   = require('https');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Байеры ──────────────────────────────────────────────
// login -> { password, name, geo, sees }
// sees = имя байера чьи лиды видит этот логин
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

function sendTelegramMessage(text) {
  if (!TG_BOT_TOKEN || !TG_CHAT_ID) return;
  const body = JSON.stringify({ chat_id: TG_CHAT_ID, text, parse_mode: 'Markdown' });
  const options = {
    hostname: 'api.telegram.org',
    path: `/bot${TG_BOT_TOKEN}/sendMessage`,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  };
  const req = https.request(options, (res) => {
    let data = '';
    res.on('data', chunk => data += chunk);
    res.on('end', () => {
      console.log('[TG response]', res.statusCode, data.slice(0, 200));
    });
  });
  req.on('error', (e) => console.error('[TG error]', e.message));
  req.write(body);
  req.end();
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
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-Auth-Token');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ── Routes ────────────────────────────────────────────────

// POST /lead — принимаем лид от лендинга
app.post('/lead', (req, res) => {
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
    createdAt: new Date().toISOString()
  };
  data.leads.unshift(lead);
  saveData(data);
  console.log(`[LEAD] ${lead.name} | ${lead.email} | ${lead.phone} | ${lead.buyer}`);

  // Telegram
  const u = lead.utms;
  const leadDate = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Kiev' });
  const utmLines = [
    u.utm_source   ? `├ source: ${u.utm_source}`         : null,
    u.utm_medium   ? `├ medium: ${u.utm_medium}`         : null,
    u.utm_campaign ? `├ campaign: ${u.utm_campaign}`     : null,
    u.utm_content  ? `├ content: ${u.utm_content}`       : null,
    u.campaign_id  ? `├ campaign_id: ${u.campaign_id}`   : null,
    u.adset_id     ? `├ adset_id: ${u.adset_id}`         : null,
    u.creative_id  ? `└ creative_id: ${u.creative_id}`   : null,
  ].filter(Boolean).join('\n') || '└ -';

  const tgLines = [
    `🔔 *НОВЫЙ ЛИД — GPT Trade*`,
    `📋 Заявка: \`${lead.id.slice(0,8).toUpperCase()}\``,
    ``,
    `👤 *КОНТАКТЫ*`,
    `├ Имя: ${lead.name}`,
    `├ Email: ${lead.email}`,
    `└ Телефон: ${lead.phone}`,
    ``,
    `👷 *БАЙЕР*`,
    `└ ${lead.buyer}`,
    ``,
    `📊 *UTM*`,
    utmLines,
    u.fbclid ? `🔗 fbclid: ${u.fbclid.slice(0,24)}...` : null,
    u.landing ? `🌐 ${u.landing}` : null,
    ``,
    `📍 IP: ${lead.ip}`,
    `⏰ ${leadDate}`,
  ].filter(l => l !== null).join('\n');

  sendTelegramMessage(tgLines);

  res.json({ ok: true, id: lead.id });
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

  // Last 7 days
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
    ['ID','Name','Email','Phone','Buyer','IP','Date','utm_source','utm_campaign','utm_content'],
    ...leads.map(l => [
      l.id, l.name, l.email, l.phone, l.buyer, l.ip,
      l.createdAt,
      l.utms?.utm_source || '',
      l.utms?.utm_campaign || '',
      l.utms?.utm_content || '',
    ])
  ].map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(','));

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="leads.csv"');
  res.send(rows.join('\n'));
});

// ── Static ────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));

// ── Start ─────────────────────────────────────────────────
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
