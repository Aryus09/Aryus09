const TelegramBot = require('node-telegram-bot-api');
const db = require('./database');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

const BOT_TOKEN = '8527431688:AAEEAkRXPc1AUkS7bkd5muw_ixKgpRM99gg';
const ADMIN_IDS = [5465056934];
const WEBAPP_URL = 'https://viu.teee.lol/nexeo/';
const SAWER_USERNAME = 'USERNAME_SAWERIA';
const API_TOKEN = 'keykeyi'; // JANGAN DIUBAH/DIHAPUS
const PAKET_CONFIG_PATH = path.join(__dirname, 'paket.json');

process.env.TZ = 'Asia/Jakarta';

const userStates = new Map();

const MESSAGES = {
  RATE_LIMIT_CMD: 'Terlalu cepat. Silakan coba lagi dalam beberapa detik.',
  RATE_LIMIT_CB: 'Aksi terlalu cepat. Silakan menunggu sebentar.',
  START_REGISTERED_TITLE:
    'Halo, {name}\n────────────────────────\nID Pengguna : {userId}\nUsername    : {username}\nBerakhir     : {date}\nStatus       : {status}\n────────────────────────',
  START_NEW_TITLE:
    'Halo, {name}\n────────────────────────\nID Pengguna : {userId}\nUsername    : {username}\nBerakhir     : -\nStatus       : Belum Terdaftar\n────────────────────────',
  ADMIN_PANEL_TITLE: 'Panel Admin',
  ADMIN_STATS_TITLE:
    'Statistik Pengguna\n────────────────────────\nTotal Pengguna : {total}\nAktif          : {active}\nExpired        : {expired}\nTrial          : {trial}',
  ALREADY_REGISTERED: 'Akun Anda sudah terdaftar dalam sistem.',
  TRIAL_ACTIVATED:
    'Trial Berhasil Diaktifkan\n\nID Pengguna : {userId}\nBerakhir     : {date}\nStatus       : Trial — 1 hari\n\nKetik /start untuk melihat status akun.',
  SUBSCRIPTION_MENU_TITLE: 'Pilih Paket Berlangganan',
  PENDING_EXISTS:
    'Anda memiliki transaksi aktif. Selesaikan atau batalkan transaksi tersebut sebelum memulai yang baru.',
  CREATING_TRANSACTION: 'Membuat transaksi — mohon menunggu.',
  PAYMENT_CAPTION:
    'Pembelian Paket\n\nPaket       : {days} hari\nHarga       : {price}\nBatas Bayar  : {expiry}\n\nTransaksi otomatis dibatalkan jika melebihi batas waktu.\nSetelah pembayaran terverifikasi, masa aktif akan diaktifkan atau diperpanjang.\n\nPerhatian:\n• Pastikan jumlah yang dibayarkan sesuai.\n• Verifikasi dilakukan secara otomatis.\n\nTidak ingin melanjutkan? Tekan tombol "Batal Pembayaran".',
  PAYMENT_WARNING_1MIN: 'Peringatan: Sisa waktu pembayaran 1 menit.',
  PAYMENT_TIMEOUT: 'Waktu pembayaran telah habis. Silakan ulangi proses dengan perintah /start.',
  PAYMENT_CANCELLED: 'Transaksi dibatalkan sesuai permintaan Anda.',
  PAYMENT_SUCCESS_USER:
    'Pembayaran Berhasil\n\nMasa berlangganan diperpanjang selama {days} hari.\nBerakhir baru : {date}\nTerima kasih.',
  PAYMENT_SUCCESS_ADMIN:
    'Pemberitahuan Pembayaran — Berhasil\nPengguna : {userId}\nPaket     : {days} hari\nJumlah    : {price}\nWaktu     : {time}',
  TRANSACTION_CREATE_FAIL:
    'Gagal membuat transaksi. Silakan coba lagi atau hubungi admin jika masalah berlanjut.',
  TRANSACTION_ERROR_GENERIC:
    'Terjadi kesalahan saat membuat transaksi. Silakan coba lagi nanti atau hubungi admin.',
  GENERIC_ERROR: 'Terjadi kesalahan. Silakan coba lagi nanti.',
  HELP_TEXT:
    'Daftar Perintah\n\n/start  — Lihat status akun\n/help   — Bantuan & daftar perintah\n\nPerintah Admin:\n/add <userid> <hari> — Tambah/perpanjang pengguna\n/delete <userid> — Hapus pengguna\n/list — Daftar semua pengguna\n/admin — Buka panel admin',
  USER_UPDATED: 'Pengguna berhasil diperbarui.\nBerakhir baru : {date}',
  USER_NOTIFY_UPDATED: 'Akun Anda telah diperbarui oleh admin.\nPenambahan masa: +{days} hari.',
  USER_ADDED: 'Pengguna ditambahkan dan diaktifkan.\nBerakhir : {date}',
  USER_DELETED: 'Pengguna {id} berhasil dihapus.',
  USER_NOTIFY_DELETED:
    'Akun Anda telah dinonaktifkan oleh admin.\nJika ini tidak sesuai, silakan hubungi admin.',
  USER_NOT_FOUND: 'Pengguna tidak ditemukan pada database.',
  DB_EMPTY: 'Tidak ada data pengguna di database.',
  LIST_HEADER: 'Daftar Pengguna ({total})\n────────────────────────',
  TECHNICAL_ISSUE: 'Terjadi gangguan teknis. Tim kami telah menerima notifikasi.'
};

const BUTTONS = {
  START_REGISTERED: [
    [{ text: 'Buat Akun via (WebApp)', web_app: { url: WEBAPP_URL } }],
    [{ text: 'Buat Akun via (Telegram Bot)', callback_data: 'createAccounts' }],
    [{ text: 'Perpanjang Paket', callback_data: 'subscription' }]
  ],
  START_NEW: [
    [{ text: 'Berlangganan', callback_data: 'subscription' }],
    [{ text: 'Coba Gratis (1 hari)', callback_data: 'trial_direct' }]
  ],
  ADMIN_PANEL: [
    [{ text: 'Daftar Pengguna', callback_data: 'admin_list_users' }],
    [{ text: 'Statistik Pengguna', callback_data: 'admin_stats' }]
  ],
  SUBSCRIPTION_PACKAGES: [
    [{ text: '15 Hari — Rp 20.000', callback_data: 'package_15_days' }],
    [{ text: '30 Hari — Rp 35.000', callback_data: 'package_30_days' }],
    [{ text: '60 Hari — Rp 60.000', callback_data: 'package_60_days' }],
    [{ text: 'Kembali', callback_data: 'back_to_start' }]
  ],
  CANCEL_PAYMENT: (trxId) => [[{ text: 'Batal Pembayaran', callback_data: `cancel_payment_${trxId}` }]]
};

class Logger {
  static getTimestamp() {
    return new Date().toLocaleString('id-ID', {
      timeZone: 'Asia/Jakarta',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  }

  static info(category, message, data = {}) {
    const timestamp = this.getTimestamp();
    console.log(`[${timestamp}] [INFO] [${category}] ${message}`);
    if (Object.keys(data).length > 0) {
      console.log('  Data:', JSON.stringify(data, null, 2));
    }
  }

  static error(category, message, error = null) {
    const timestamp = this.getTimestamp();
    console.error(`[${timestamp}] [ERROR] [${category}] ${message}`);
    if (error) {
      console.error('  Error:', error.message || error);
      if (error.stack) console.error('  Stack:', error.stack);
    }
  }

  static warning(category, message, data = {}) {
    const timestamp = this.getTimestamp();
    console.warn(`[${timestamp}] [WARNING] [${category}] ${message}`);
    if (Object.keys(data).length > 0) {
      console.warn('  Data:', JSON.stringify(data, null, 2));
    }
  }

  static success(category, message, data = {}) {
    const timestamp = this.getTimestamp();
    console.log(`[${timestamp}] [SUCCESS] [${category}] ${message}`);
    if (Object.keys(data).length > 0) {
      console.log('  Data:', JSON.stringify(data, null, 2));
    }
  }

  static payment(action, userId, data = {}) {
    const timestamp = this.getTimestamp();
    console.log(`[${timestamp}] [PAYMENT] [${action}] User: ${userId}`);
    if (Object.keys(data).length > 0) console.log('  Details:', JSON.stringify(data, null, 2));
  }
}

const bot = new TelegramBot(BOT_TOKEN, { polling: true });
Logger.info('SYSTEM', 'Bot initialized successfully');

class UserSessionManager {
  constructor() {
    this.sessions = new Map();
    this.pendingPayments = new Map();
    this.rateLimits = new Map();
  }

  checkRateLimit(userId, type) {
    const now = Date.now();
    const delays = { cmd: 3000, cb: 1000 };

    if (!this.rateLimits.has(userId)) this.rateLimits.set(userId, { cmd: 0, cb: 0 });

    const data = this.rateLimits.get(userId);

    if (now - data[type] < delays[type]) {
      return false;
    }

    data[type] = now;
    return true;
  }

  hasPendingPayment(userId) {
    for (const [, data] of this.pendingPayments) {
      if (data.userId === userId) {
        Logger.info('PAYMENT', `User ${userId} has pending payment`, { transactionId: data.transactionId });
        return true;
      }
    }
    return false;
  }

  addPendingPayment(transactionId, userId, packageKey) {
    this.pendingPayments.set(transactionId, {
      userId,
      packageKey,
      timestamp: Date.now(),
      transactionId,
      interval: null
    });

    Logger.payment('CREATED', userId, { transactionId, package: packageKey, timestamp: new Date().toISOString() });
  }

  removePendingPayment(transactionId) {
    const payment = this.pendingPayments.get(transactionId);
    if (payment) {
      if (payment.interval) {
        clearInterval(payment.interval);
        Logger.info('PAYMENT', `Monitoring interval cleared for transaction ${transactionId}`);
      }
      Logger.payment('REMOVED', payment.userId, { transactionId });
      this.pendingPayments.delete(transactionId);
      return true;
    }
    return false;
  }

  getPendingPayment(transactionId) {
    return this.pendingPayments.get(transactionId);
  }

  cleanupOldSessions() {
    const now = Date.now();
    const maxAge = 30 * 60 * 1000;

    for (const [transactionId, data] of this.pendingPayments) {
      if (now - data.timestamp > maxAge) {
        Logger.warning('CLEANUP', `Removing expired payment session`, {
          transactionId,
          userId: data.userId,
          age: Math.floor((now - data.timestamp) / 60000) + ' minutes'
        });
        this.removePendingPayment(transactionId);
      }
    }
  }
}

const sessionManager = new UserSessionManager();

setInterval(() => sessionManager.cleanupOldSessions(), 10 * 60 * 1000);

const MEMBERSHIP_PACKAGES = {
  '15_days': { duration: 15, amount: 20000 },
  '30_days': { duration: 30, amount: 35000 },
  '60_days': { duration: 60, amount: 60000 }
};

function formatRupiah(amount) {
  return 'Rp ' + amount.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
}

function getExpiryTime(minutes = 5) {
  const now = new Date();
  const expiry = new Date(now.getTime() + minutes * 60000);
  return expiry.toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit' });
}

function getFullExpiry(minutes = 5) {
  const now = new Date();
  const expiry = new Date(now.getTime() + minutes * 60000);

  const day = String(expiry.getDate()).padStart(2, '0');
  const month = String(expiry.getMonth() + 1).padStart(2, '0');
  const year = expiry.getFullYear();

  const hour = String(expiry.getHours()).padStart(2, '0');
  const minute = String(expiry.getMinutes()).padStart(2, '0');

  return `${day}/${month}/${year} ${hour}:${minute} WIB (${minutes} Menit)`;
}

async function loadPaketConfig() {
  try {
    const data = await fs.readFile(PAKET_CONFIG_PATH, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    Logger.error('CONFIG', 'Failed to load paket.json', error);
    return null;
  }
}

async function buildPackageKeyboard() {
  const config = await loadPaketConfig();
  const keyboard = [];

  if (!config) return { inline_keyboard: [[{ text: 'Kembali', callback_data: 'back_to_start' }]] };

  if (config.lifetime) {
    keyboard.push([{ text: `${config.lifetime.label}`, callback_data: 'sel_pkg_lifetime' }]);
  }

  if (config.exp && Array.isArray(config.exp)) {
    config.exp.forEach((item, index) => {
      keyboard.push([{ text: `${item.label}`, callback_data: `sel_pkg_exp_${index}` }]);
    });
  }

  if (config.custom && config.custom.enabled) {
    keyboard.push([{ text: `${config.custom.label}`, callback_data: 'sel_pkg_custom' }]);
  }

  keyboard.push([{ text: 'Kembali', callback_data: 'back_to_start' }]);

  return { inline_keyboard: keyboard };
}

async function createPaymentTransaction(amount) {
  try {
    Logger.payment('API_REQUEST', 'system', { endpoint: 'create', amount: formatRupiah(amount) });

    const response = await axios.post(
      'https://sawerin.teee.lol/api/create',
      { username: SAWER_USERNAME, amount },
      { headers: { 'Content-Type': 'application/json' }, timeout: 10000 }
    );

    Logger.payment('API_RESPONSE', 'system', {
      endpoint: 'create',
      success: response.data.success,
      transactionId: response.data.data?.transaction_id
    });

    return response.data;
  } catch (error) {
    Logger.error('PAYMENT_API', 'Failed to create transaction', error);
    throw error;
  }
}

async function checkPaymentStatus(transactionId) {
  try {
    Logger.payment('STATUS_CHECK', 'system', { transactionId });

    const response = await axios.get(`https://sawerin.teee.lol/api/status/${transactionId}`, { timeout: 10000 });

    const status = response.data.data?.status || 'Unknown';

    Logger.payment('STATUS_RESPONSE', 'system', { transactionId, status, success: response.data.success });

    return response.data;
  } catch (error) {
    Logger.error('PAYMENT_API', `Failed to check status for ${transactionId}`, error);
    return null;
  }
}

async function monitorPayment(chatId, userId, transactionId, packageKey, messageId) {
  const pkg = MEMBERSHIP_PACKAGES[packageKey];
  const start = Date.now();
  const max = 5 * 60000;
  const warnTime = 4 * 60000;
  let warned = false;
  let checkCount = 0;

  Logger.payment('MONITOR_START', userId, {
    transactionId,
    package: packageKey,
    duration: `${pkg.duration} days`,
    amount: formatRupiah(pkg.amount),
    expiresIn: '5 minutes'
  });

  const interval = setInterval(async () => {
    const elapsed = Date.now() - start;
    checkCount++;

    if (elapsed >= max) {
      clearInterval(interval);
      sessionManager.removePendingPayment(transactionId);

      Logger.payment('TIMEOUT', userId, { transactionId, checksPerformed: checkCount, duration: '5 minutes' });

      try {
        await bot.deleteMessage(chatId, messageId);
      } catch (e) {}

      bot.sendMessage(chatId, MESSAGES.PAYMENT_TIMEOUT);
      return;
    }

    if (elapsed >= warnTime && !warned) {
      warned = true;
      bot.sendMessage(chatId, MESSAGES.PAYMENT_WARNING_1MIN);
    }

    const status = await checkPaymentStatus(transactionId);

    if (status && status.success && status.data.status === 'Success') {
      clearInterval(interval);
      sessionManager.removePendingPayment(transactionId);

      try {
        const existing = await db.getUser(userId);
        const now = new Date();
        let base;

        if (existing) {
          const expiry = new Date(existing.expiredAt);
          base = expiry > now ? expiry : now;
        } else {
          base = now;
        }

        const newExpiry = new Date(base.getTime() + pkg.duration * 86400000);

        if (existing) {
          const dbData = await db.loadDatabase();
          dbData.users[userId].expiredAt = newExpiry.toISOString();
          dbData.users[userId].isTrial = false;
          await db.saveDatabase(dbData);
        } else {
          await db.addUser(userId, '', pkg.duration);
        }

        try {
          await bot.deleteMessage(chatId, messageId);
        } catch (e) {}

        bot.sendMessage(
          chatId,
          MESSAGES.PAYMENT_SUCCESS_USER
            .replace('{days}', pkg.duration)
            .replace('{date}', db.formatDate(newExpiry.toISOString()))
        );

        for (const admin of ADMIN_IDS) {
          bot.sendMessage(
            admin,
            MESSAGES.PAYMENT_SUCCESS_ADMIN
              .replace('{userId}', userId)
              .replace('{days}', pkg.duration)
              .replace('{price}', formatRupiah(pkg.amount))
              .replace('{time}', Logger.getTimestamp())
          );
        }
      } catch (error) {
        Logger.error('PAYMENT', `Failed to process successful payment for user ${userId}`, error);
      }
    }
  }, 5000);

  const payment = sessionManager.getPendingPayment(transactionId);
  if (payment) {
    payment.interval = interval;
  }
}

const PAGE_SIZE = 15;
function buildUsersPage(usersObj, page = 1) {
  const ids = Object.keys(usersObj)
    .map((x) => x.toString())
    .sort((a, b) => Number(a) - Number(b));
  const total = ids.length;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  page = Math.max(1, Math.min(page, totalPages));

  const start = (page - 1) * PAGE_SIZE;
  const end = Math.min(start + PAGE_SIZE, total);

  let text = `Daftar Pengguna (${total})\n────────────────────────\n\n`;
  for (let i = start; i < end; i++) {
    const id = ids[i];
    const u = usersObj[id];
    const expired = db.isExpired(u.expiredAt);
    const status = expired ? 'Expired' : 'Active';
    const type = u.isTrial ? 'Trial' : 'Member';

    text += `ID: <code>${id}</code>\nUsername: ${u.username || '-'}\nBerakhir: ${db.formatDate(
      u.expiredAt
    )}\nStatus: ${status} (${type})\n\n`;
  }

  text += `Halaman ${page} / ${totalPages}`;
  return { text, page, totalPages };
}

function buildListKeyboard(page, totalPages) {
  const row = [];
  if (page > 1) row.push({ text: '◀️ Prev', callback_data: `list_page_${page - 1}` });
  row.push({ text: 'Kembali', callback_data: 'back_to_start' });
  if (page < totalPages) row.push({ text: 'Next ▶️', callback_data: `list_page_${page + 1}` });
  return { inline_keyboard: [row] };
}

bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const text = msg.text;

  if (userStates.has(userId)) {
    const state = userStates.get(userId);

    if (text.startsWith('/')) {
      userStates.delete(userId);
      return;
    }

    const user = await db.getUser(userId);
    if (!user) {
      bot.sendMessage(msg.chat.id, 'Sesi tidak valid.');
      userStates.delete(userId);
      return;
    }

    const parts = text.split(',').map((s) => s.trim());

    let domain, password, phone;

    if (state.type === 'custom') {
      if (parts.length < 3) {
        return bot.sendMessage(
          msg.chat.id,
          'Format salah.\nFormat: domain,password,phone\nContoh: gmail.com,Masuk123,628123456'
        );
      }
      [domain, password, phone] = parts;
    } else {
      if (parts.length < 2) {
        return bot.sendMessage(
          msg.chat.id,
          'Format salah.\nFormat: domain,password\nContoh: gmail.com,Masuk123'
        );
      }
      [domain, password] = parts;
    }

    const cooldown = await db.checkCooldown(userId);
    if (!cooldown.allowed) {
      let msgCooldown;
      if (user.isTrial) {
        msgCooldown = `⏳ Akun trial memiliki cooldown 2 jam.\nTunggu ${Math.ceil(
          cooldown.remaining / 60
        )} menit lagi.`;
      } else {
        msgCooldown = `Tunggu ${cooldown.remaining} detik lagi.`;
      }
      return bot.sendMessage(msg.chat.id, msgCooldown);
    }

    let amount = user.isTrial ? 1 : 10;
    let finalPhone = phone;
    const config = await loadPaketConfig();

    if (state.type === 'exp') {
      const expPkg = config.exp[state.pkgIndex];
      if (expPkg) finalPhone = expPkg.phone;
    }

    const processMsg = await bot.sendMessage(msg.chat.id, '⏳ Sedang memproses... mohon tunggu.');

    try {
      let apiUrl;

      if (state.type === 'lifetime') {
        const paketNum = config.lifetime?.paket || 200;
        apiUrl = `https://viu.teee.lol/?domain=${encodeURIComponent(
          domain
        )}&password=${encodeURIComponent(password)}&amount=${amount}&paket=${paketNum}&token=${encodeURIComponent(
          API_TOKEN
        )}`;
      } else {
        if (!finalPhone) finalPhone = '628123456789';
        apiUrl = `https://viu.teee.lol/?domain=${encodeURIComponent(
          domain
        )}&password=${encodeURIComponent(password)}&amount=${amount}&phone=${encodeURIComponent(
          finalPhone
        )}&token=${encodeURIComponent(API_TOKEN)}`;
      }

      const response = await axios.get(apiUrl, { timeout: 60000 });

      if (response.data && response.data.results) {
        let content = 'VIU PREMIUM RESULTS (BOT GENERATED)\n';
        content += `Date: ${new Date().toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' })}\n`;
        content += '='.repeat(50) + '\n\n';

        let successCount = 0;
        for (const res of response.data.results) {
          if (res.status === 'success') {
            content += `${res.email} | ${res.password} | ${res.durasi}\n`;
            successCount++;
          }
        }

        if (successCount === 0) {
          await bot.sendMessage(
            msg.chat.id,
            'Gagal membuat akun. Silakan cek domain/password atau coba lagi.'
          );
        } else {
          const buffer = Buffer.from(content, 'utf8');
          await bot.sendDocument(
            msg.chat.id,
            buffer,
            {
              caption: `✅ Berhasil membuat ${successCount} akun.`
            },
            {
              filename: `result_${userId}_${Date.now()}.txt`,
              contentType: 'text/plain'
            }
          );

          await db.updateLastGenerate(userId);
        }
      } else {
        throw new Error('Invalid API response');
      }
    } catch (err) {
      Logger.error('GENERATE', `Failed for user ${userId}`, err);
      bot.sendMessage(msg.chat.id, 'Terjadi kesalahan saat menghubungi server generator.');
    } finally {
      try {
        await bot.deleteMessage(msg.chat.id, processMsg.message_id);
      } catch (e) {}
      userStates.delete(userId);
    }
    return;
  }

  if (!sessionManager.checkRateLimit(userId, 'cmd')) {
    bot.sendMessage(msg.chat.id, MESSAGES.RATE_LIMIT_CMD);
    return;
  }
});

bot.onText(/\/start/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const name = msg.from.first_name || '';
  const username = msg.from.username || '';

  Logger.info('COMMAND', `/start executed by user ${userId} (@${username})`);

  try {
    const user = await db.getUser(userId);
    let message;

    if (user) {
      const status = user.isTrial ? 'Trial' : db.isExpired(user.expiredAt) ? 'Expired' : 'Aktif';
      message = MESSAGES.START_REGISTERED_TITLE.replace('{name}', name)
        .replace('{userId}', userId)
        .replace('{username}', username ? '@' + username : '-')
        .replace('{date}', db.formatDate(user.expiredAt))
        .replace('{status}', status);

      await bot.sendMessage(chatId, message, { reply_markup: { inline_keyboard: BUTTONS.START_REGISTERED } });
    } else {
      message = MESSAGES.START_NEW_TITLE.replace('{name}', name)
        .replace('{userId}', userId)
        .replace('{username}', username ? '@' + username : '-');

      await bot.sendMessage(chatId, message, { reply_markup: { inline_keyboard: BUTTONS.START_NEW } });
    }
  } catch (error) {
    Logger.error('COMMAND', `/start failed for user ${userId}`, error);
    bot.sendMessage(chatId, MESSAGES.GENERIC_ERROR);
  }
});

bot.onText(/\/admin/, (msg) => {
  const userId = msg.from.id;
  if (!ADMIN_IDS.includes(userId)) return;
  bot.sendMessage(msg.chat.id, MESSAGES.ADMIN_PANEL_TITLE, {
    reply_markup: { inline_keyboard: BUTTONS.ADMIN_PANEL }
  });
});

bot.on('callback_query', async (q) => {
  const chatId = q.message.chat.id;
  const messageId = q.message.message_id;
  const userId = q.from.id;
  const data = q.data;

  if (!sessionManager.checkRateLimit(userId, 'cb')) {
    bot.answerCallbackQuery(q.id, { text: MESSAGES.RATE_LIMIT_CB, show_alert: true });
    return;
  }

  try {
    if (data === 'createAccounts') {
      Logger.info('MENU', `User ${userId} opened Generate Menu`);

      const user = await db.getUser(userId);
      if (!user || db.isExpired(user.expiredAt)) {
        bot.answerCallbackQuery(q.id, {
          text: 'Akses ditolak. Akun expired atau belum terdaftar.',
          show_alert: true
        });
        return;
      }

      const keyboard = await buildPackageKeyboard();
      await bot.editMessageText('Silahkan pilih tipe paket:', {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: keyboard
      });
      bot.answerCallbackQuery(q.id);
      return;
    }

    if (data === 'back_to_gen') {
      const keyboard = await buildPackageKeyboard();
      await bot.editMessageText('Silahkan pilih tipe paket:', {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: keyboard
      });
      userStates.delete(userId);
      bot.answerCallbackQuery(q.id);
      return;
    }

    if (data.startsWith('sel_pkg_')) {
      const pkgType = data.replace('sel_pkg_', '');
      let instruction = '';
      let example = '';

      const stateObj = { step: 'awaiting_input' };

      if (pkgType === 'lifetime') {
        stateObj.type = 'lifetime';
        instruction = 'Masukan domain, password';
        example = 'Contoh: gmail.com,Masuk123';
      } else if (pkgType === 'custom') {
        stateObj.type = 'custom';
        instruction = 'Masukan domain, password, phone';
        example = 'Contoh: gmail.com,Masuk123,628123456789';
      } else if (pkgType.startsWith('exp_')) {
        stateObj.type = 'exp';
        stateObj.pkgIndex = parseInt(pkgType.replace('exp_', ''));
        instruction = 'Masukan domain, password';
        example = 'Contoh: gmail.com,Masuk123';
      }

      userStates.set(userId, stateObj);

      const backBtn = { inline_keyboard: [[{ text: 'Kembali', callback_data: 'back_to_gen' }]] };

      await bot.editMessageText(`${instruction}\n${example}`, {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: backBtn
      });

      bot.answerCallbackQuery(q.id);
      return;
    }

    if (data && data.startsWith('list_page_')) {
      if (!ADMIN_IDS.includes(userId)) return;
      const requestedPage = parseInt(data.replace('list_page_', ''), 10) || 1;
      const users = await db.getAllUsers();
      const pageData = buildUsersPage(users, requestedPage);
      const keyboard = buildListKeyboard(pageData.page, pageData.totalPages);

      await bot.editMessageText(pageData.text, {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: keyboard,
        parse_mode: 'HTML',
        disable_web_page_preview: true
      });
      bot.answerCallbackQuery(q.id);
      return;
    }

    if (data === 'admin_stats') {
      const users = await db.getAllUsers();
      const total = Object.keys(users).length;
      let active = 0,
        expired = 0,
        trial = 0;
      for (const id in users) {
        if (users[id].isTrial) trial++;
        if (db.isExpired(users[id].expiredAt)) expired++;
        else active++;
      }
      await bot.editMessageText(
        MESSAGES.ADMIN_STATS_TITLE.replace('{total}', total)
          .replace('{active}', active)
          .replace('{expired}', expired)
          .replace('{trial}', trial),
        { chat_id: chatId, message_id: messageId }
      );
      bot.answerCallbackQuery(q.id);
      return;
    }

    if (data === 'trial_direct') {
      bot.answerCallbackQuery(q.id);
      const exist = await db.getUser(userId);
      if (exist) {
        bot.sendMessage(chatId, MESSAGES.ALREADY_REGISTERED);
        return;
      }
      const trial = await db.addTrialUser(userId, q.from.username, 1);
      await bot.editMessageText(
        MESSAGES.TRIAL_ACTIVATED.replace('{userId}', userId).replace(
          '{date}',
          db.formatDate(trial.expiredAt)
        ),
        { chat_id: chatId, message_id: messageId }
      );
      return;
    }

    if (data === 'subscription') {
      await bot.editMessageText('Pilih paket berlangganan:', {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: { inline_keyboard: BUTTONS.SUBSCRIPTION_PACKAGES }
      });
      bot.answerCallbackQuery(q.id);
      return;
    }

    if (data === 'back_to_start') {
      bot.answerCallbackQuery(q.id);
      userStates.delete(userId);
      const user = await db.getUser(userId);
      const name = q.from.first_name || '';
      const username = q.from.username || '';
      let msg;

      if (user) {
        let status = user.isTrial ? 'Trial' : db.isExpired(user.expiredAt) ? 'Expired' : 'Aktif';
        msg = MESSAGES.START_REGISTERED_TITLE.replace('{name}', name)
          .replace('{userId}', userId)
          .replace('{username}', username ? '@' + username : '-')
          .replace('{date}', db.formatDate(user.expiredAt))
          .replace('{status}', status);
        await bot.editMessageText(msg, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: { inline_keyboard: BUTTONS.START_REGISTERED }
        });
      } else {
        msg = MESSAGES.START_NEW_TITLE.replace('{name}', name)
          .replace('{userId}', userId)
          .replace('{username}', username ? '@' + username : '-');
        await bot.editMessageText(msg, {
          chat_id: chatId,
          message_id: messageId,
          reply_markup: { inline_keyboard: BUTTONS.START_NEW }
        });
      }
      return;
    }

    if (data.startsWith('cancel_payment_')) {
      const trxId = data.replace('cancel_payment_', '');
      sessionManager.removePendingPayment(trxId);
      try {
        await bot.deleteMessage(chatId, messageId);
      } catch (e) {}
      bot.sendMessage(chatId, MESSAGES.PAYMENT_CANCELLED);
      bot.emit('message', { chat: { id: chatId }, from: q.from, text: '/start' });
      return;
    }

    if (data.startsWith('package_')) {
      if (sessionManager.hasPendingPayment(userId)) {
        bot.answerCallbackQuery(q.id, { text: MESSAGES.PENDING_EXISTS, show_alert: true });
        return;
      }
      const key = data.replace('package_', '');
      const pkg = MEMBERSHIP_PACKAGES[key];

      await bot.editMessageText(MESSAGES.CREATING_TRANSACTION, {
        chat_id: chatId,
        message_id: messageId
      });

      try {
        const pay = await createPaymentTransaction(pkg.amount);
        if (!pay.success) {
          bot.sendMessage(chatId, MESSAGES.TRANSACTION_CREATE_FAIL);
          return;
        }
        const trxId = pay.data.transaction_id;
        const qrImage = pay.data.qr_image;
        sessionManager.addPendingPayment(trxId, userId, key);
        const caption = MESSAGES.PAYMENT_CAPTION.replace('{days}', pkg.duration)
          .replace('{price}', formatRupiah(pkg.amount))
          .replace('{expiry}', getFullExpiry(5));

        await bot.editMessageMedia(
          { type: 'photo', media: qrImage, caption: caption },
          {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: { inline_keyboard: BUTTONS.CANCEL_PAYMENT(trxId) }
          }
        );
        monitorPayment(chatId, userId, trxId, key, messageId);
      } catch (err) {
        bot.sendMessage(chatId, MESSAGES.TRANSACTION_ERROR_GENERIC);
      }
      bot.answerCallbackQuery(q.id);
      return;
    }

    bot.answerCallbackQuery(q.id);
  } catch (error) {
    Logger.error('CALLBACK', `Error handling callback from user ${userId}`, error);
    bot.answerCallbackQuery(q.id, { text: MESSAGES.GENERIC_ERROR, show_alert: true });
  }
});

bot.onText(/\/add (\d+) (\d+)/, async (msg, match) => {
  const adminId = msg.from.id;
  if (!ADMIN_IDS.includes(adminId)) return;
  const target = parseInt(match[1]);
  const days = parseInt(match[2]);

  try {
    const user = await db.getUser(target);
    const now = new Date();
    let newExpiry;

    if (user) {
      const expiry = new Date(user.expiredAt);
      const base = expiry > now ? expiry : now;
      newExpiry = new Date(base.getTime() + days * 86400000);
      const dbData = await db.loadDatabase();
      dbData.users[target].expiredAt = newExpiry.toISOString();
      dbData.users[target].isTrial = false;
      await db.saveDatabase(dbData);
    } else {
      const newUser = await db.addUser(target, '', days);
      newExpiry = newUser.expiredAt;
    }
    bot.sendMessage(msg.chat.id, MESSAGES.USER_UPDATED.replace('{date}', db.formatDate(newExpiry)));
    try {
      bot.sendMessage(target, MESSAGES.USER_NOTIFY_UPDATED.replace('{days}', days));
    } catch (e) {}
  } catch (err) {
    bot.sendMessage(msg.chat.id, 'Gagal: ' + err.message);
  }
});

bot.onText(/\/delete (\d+)/, async (msg, match) => {
  const adminId = msg.from.id;
  if (!ADMIN_IDS.includes(adminId)) return;
  const target = parseInt(match[1]);
  const deleted = await db.deleteUser(target);
  if (deleted) {
    bot.sendMessage(msg.chat.id, MESSAGES.USER_DELETED.replace('{id}', target));
    try {
      bot.sendMessage(target, MESSAGES.USER_NOTIFY_DELETED);
    } catch (e) {}
  } else {
    bot.sendMessage(msg.chat.id, MESSAGES.USER_NOT_FOUND);
  }
});

bot.onText(/\/broadcast/, async (msg) => {
  const adminId = msg.from.id;
  const chatId = msg.chat.id;

  if (!ADMIN_IDS.includes(adminId)) return;

  if (!msg.reply_to_message) {
    return bot.sendMessage(chatId, '⚠️ <b>Cara Broadcast yang Benar:</b>\n\n1. Ketik pesan (bisa text, gambar, video, caption).\n2. Format text sesuka Anda (Bold/Italic).\n3. Kirim pesan itu ke sini.\n4. <b>Reply</b> pesan tersebut dan ketik <code>/broadcast</code>', { parse_mode: 'HTML' });
  }

  const messageToCopy = msg.reply_to_message.message_id;

  Logger.info('BROADCAST', `Admin ${adminId} started broadcast via Reply`);
  const statusMsg = await bot.sendMessage(chatId, '⏳ <b>Memulai broadcast...</b>', { parse_mode: 'HTML' });

  try {
    const users = await db.getAllUsers();
    const userIds = Object.keys(users);
    const total = userIds.length;

    let success = 0;
    let blocked = 0;
    let failed = 0;

    for (const targetId of userIds) {
      try {
        await bot.copyMessage(targetId, chatId, messageToCopy);
        
        success++;
        await new Promise(resolve => setTimeout(resolve, 50));

      } catch (error) {
        if (error.response && error.response.statusCode === 403) {
          blocked++;
        } else {
          failed++;
        }
      }
    }

    const report = `<b>📢 Laporan Broadcast Selesai</b>\n` +
                   `────────────────────────\n` +
                   `👥 Total Target : ${total}\n` +
                   `✅ Berhasil     : ${success}\n` +
                   `🚫 Diblokir Bot : ${blocked}\n` +
                   `❌ Gagal Lainnya: ${failed}`;

    await bot.editMessageText(report, {
      chat_id: chatId,
      message_id: statusMsg.message_id,
      parse_mode: 'HTML'
    });

    Logger.success('BROADCAST', `Finished. Success: ${success}, Blocked: ${blocked}`);

  } catch (err) {
    Logger.error('BROADCAST', 'General error', err);
    bot.sendMessage(chatId, '❌ Terjadi kesalahan sistem saat broadcast.');
  }
});

bot.onText(/\/list/, async (msg) => {
  const adminId = msg.from.id;
  if (!ADMIN_IDS.includes(adminId)) return;
  const users = await db.getAllUsers();
  const ids = Object.keys(users);
  if (ids.length === 0) return bot.sendMessage(msg.chat.id, MESSAGES.DB_EMPTY);
  const pageData = buildUsersPage(users, 1);
  const keyboard = buildListKeyboard(pageData.page, pageData.totalPages);
  await bot.sendMessage(msg.chat.id, pageData.text, {
    reply_markup: keyboard,
    parse_mode: 'HTML',
    disable_web_page_preview: true
  });
});

bot.onText(/\/help/, (msg) => {
  bot.sendMessage(msg.chat.id, MESSAGES.HELP_TEXT);
});

bot.on('polling_error', (error) => {});
process.on('uncaughtException', (error) => {});
process.on('unhandledRejection', (reason, promise) => {});

Logger.success('SYSTEM', 'BOT STARTED SUCCESSFULLY');
console.log('Bot berjalan dengan baik!');
