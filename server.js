const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
// ── Байеры: логин → { password, name, geo }
// Добавляй новых байеров сюда или через ENV BUYERS_JSON
const DEFAULT_BUYERS = {
  'gpttrade':  { password: 'pass123', name: 'pumba',    geo: 'MX', sees: 'pumba'    },
  'nepravda':  { password: 'pass456', name: 'nepravda', geo: 'MX', sees: 'nepravda' }
};
const BUYERS = process.env.BUYERS_JSON
  ? JSON.parse(process.env.BUYERS_JSON)
  : DEFAULT_BUYERS;

// Суперадмин — видит ВСЕ лиды
const SUPER_LOGIN    = process.env.SUPER_LOGIN    || 'admin';
const SUPER_PASSWORD = process.env.SUPER_PASSWORD || 'supersecret2025';
const DATA_FILE = path.join(__dirname, 'leads.json');

app.use(express.json());

// CORS — разрешаем любой origin (netlify домен)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ── helpers ──────────────────────────────────────────────────────────────────
function loadData() {
  if (!fs.existsSync(DATA_FILE)) return { leads: [], visits: 0 };
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
  catch { return { leads: [], visits: 0 }; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

// Возвращает { login, name, geo, isSuper } или null
function authenticateBuyer(req) {
  const auth = req.headers.authorization || '';
  const token = auth.replace('Bearer ', '');
  // формат токена: "login:password"
  const [login, ...rest] = token.split(':');
  const password = rest.join(':');
  if (!login || !password) return null;
  if (login === SUPER_LOGIN && password === SUPER_PASSWORD) {
    return { login: SUPER_LOGIN, name: 'Admin', geo: null, isSuper: true };
  }
  const buyer = BUYERS[login];
  if (buyer && buyer.password === password) {
    return { login, name: buyer.name, geo: buyer.geo, sees: buyer.sees, isSuper: false };
  }
  return null;
}

function authMiddleware(req, res, next) {
  const buyer = authenticateBuyer(req);
  if (!buyer) return res.status(401).json({ error: 'Unauthorized' });
  req.buyer = buyer;
  next();
}

// ── PUBLIC ENDPOINTS (вызываются с сайта) ────────────────────────────────────

// POST /lead — сохранить лид
app.post('/lead', (req, res) => {
  const { name, email, phone } = req.body;
  if (!name && !email && !phone) return res.status(400).json({ error: 'Empty lead' });

  const data = loadData();
  const { buyer: buyerField } = req.body;
  const lead = {
    id: crypto.randomUUID(),
    name: name || '',
    email: email || '',
    phone: phone || '',
    buyer: buyerField || 'unknown',
    ip: req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || '',
    ua: req.headers['user-agent'] || '',
    createdAt: new Date().toISOString()
  };
  data.leads.unshift(lead); // новые сверху
  saveData(data);

  console.log(`[LEAD] ${lead.name} | ${lead.email} | ${lead.phone}`);
  res.json({ ok: true, id: lead.id });
});

// POST /visit — трекать визит (pageview)
app.post('/visit', (req, res) => {
  const data = loadData();
  data.visits = (data.visits || 0) + 1;
  saveData(data);
  res.json({ ok: true });
});

// ── ADMIN ENDPOINTS (защищены паролем) ───────────────────────────────────────

// POST /admin/auth
app.post('/admin/auth', (req, res) => {
  const { login, password } = req.body;
  if (!login || !password) return res.status(400).json({ error: 'login and password required' });
  // Суперадмин
  if (login === SUPER_LOGIN && password === SUPER_PASSWORD) {
    return res.json({ ok: true, token: login+':'+password, name: 'Admin', isSuper: true });
  }
  const buyer = BUYERS[login];
  if (!buyer || buyer.password !== password) return res.status(401).json({ error: 'Wrong credentials' });
  res.json({ ok: true, token: login+':'+password, name: buyer.name, geo: buyer.geo, sees: buyer.sees, isSuper: false });
});

// GET /admin/stats — байер видит только свои метрики
app.get('/admin/stats', authMiddleware, (req, res) => {
  const data = loadData();
  let leads = data.leads || [];
  if (!req.buyer.isSuper) {
    var sees = req.buyer.sees || req.buyer.name;
    leads = leads.filter(l => l.buyer === sees);
  }
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  const weekAgo = new Date(now - 7 * 86400000);

  const todayLeads = leads.filter(l => l.createdAt.startsWith(today)).length;
  const weekLeads = leads.filter(l => new Date(l.createdAt) >= weekAgo).length;

  // По дням для графика (последние 7 дней)
  const byDay = {};
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now - i * 86400000).toISOString().slice(0, 10);
    byDay[d] = 0;
  }
  leads.forEach(l => {
    const d = l.createdAt.slice(0, 10);
    if (d in byDay) byDay[d]++;
  });

  res.json({
    total: leads.length,
    visits: data.visits || 0,
    today: todayLeads,
    week: weekLeads,
    cr: data.visits > 0 ? ((leads.length / data.visits) * 100).toFixed(1) : '0.0',
    byDay
  });
});

// GET /admin/leads — байер видит только свои лиды, суперадмин — все
app.get('/admin/leads', authMiddleware, (req, res) => {
  const data = loadData();
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 50;
  const search = (req.query.search || '').toLowerCase();

  let leads = data.leads || [];

  // Фильтр по байеру (не суперадмин)
  if (!req.buyer.isSuper) {
    var sees = req.buyer.sees || req.buyer.name;
    leads = leads.filter(l => l.buyer === sees);
  }

  if (search) {
    leads = leads.filter(l =>
      (l.name||'').toLowerCase().includes(search) ||
      (l.email||'').toLowerCase().includes(search) ||
      (l.phone||'').includes(search)
    );
  }

  const total = leads.length;
  const start = (page - 1) * limit;
  res.json({ leads: leads.slice(start, start + limit), total, page, pages: Math.ceil(total / limit) });
});

// DELETE /admin/lead/:id
app.delete('/admin/lead/:id', authMiddleware, (req, res) => {
  const data = loadData();
  data.leads = (data.leads || []).filter(l => l.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// GET /admin/export.csv
app.get('/admin/export.csv', authMiddleware, (req, res) => {
  const data = loadData();
  const leads = data.leads || [];
  const rows = [['ID', 'Name', 'Email', 'Phone', 'IP', 'Date']];
  leads.forEach(l => rows.push([l.id, l.name, l.email, l.phone, l.ip, l.createdAt]));
  const csv = rows.map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n');
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename="leads.csv"');
  res.send(csv);
});

// ── ADMIN SPA ─────────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'admin.html')));

// ── START ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => console.log(`GPT Trade Admin running on :${PORT}`));
