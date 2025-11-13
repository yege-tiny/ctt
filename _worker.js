let BOT_TOKENS = [];
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isInitialized = false;

const processedMessages = new Set();
const processedCallbacks = new Set();

const topicCreationLocks = new Map();

const settingsCache = new Map([
  ['verification_enabled', null],
  ['user_raw_enabled', null]
]);

class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }
  get(key) {
    const value = this.cache.get(key);
    if (value !== undefined) {
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }
  set(key, value) {
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, value);
  }
  clear() {
    this.cache.clear();
  }
}

const userInfoCache = new LRUCache(1000);      // key: real chatId
const topicIdCache = new LRUCache(1000);       // key: real chatId
const userStateCache = new LRUCache(1000);     // key: userKey = `${botIndex}:${chatId}`
const messageRateCache = new LRUCache(1000);   // key: userKey

// 生成 userKey：同一个用户在不同 bot 上有不同的状态
function makeUserKey(botIndex, chatId) {
  return `${botIndex}:${chatId}`;
}

function tgApiUrl(botToken, method) {
  return `https://api.telegram.org/bot${botToken}/${method}`;
}

export default {
  async fetch(request, env) {
    // 多个 bot token: BOT_TOKEN_ENV = "token1,token2,token3"
    const botTokenEnv = env.BOT_TOKEN_ENV || '';
    BOT_TOKENS = botTokenEnv
      .split(',')
      .map(t => t.trim())
      .filter(t => t.length > 0);

    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV
      ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV, 10)
      : 40;

    if (!env.D1) {
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    if (!BOT_TOKENS.length || !GROUP_ID) {
      return new Response('Server configuration error: Missing required environment variables', { status: 500 });
    }

    if (!isInitialized) {
      await initialize(env.D1, request, env);
      isInitialized = true;
    }

    async function handleRequest(request) {
      const url = new URL(request.url);

      if (url.pathname === '/webhook') {
        // 解析当前是第几个 bot
        let botIndex = 0;
        const botParam = url.searchParams.get('bot');
        if (botParam !== null) {
          const parsed = parseInt(botParam, 10);
          if (!Number.isNaN(parsed) && parsed >= 0 && parsed < BOT_TOKENS.length) {
            botIndex = parsed;
          }
        }
        const botToken = BOT_TOKENS[botIndex];

        try {
          const update = await request.json();
          await handleUpdate(update, botIndex, botToken, env);
          return new Response('OK');
        } catch (error) {
          console.error('handleRequest error:', error);
          return new Response('Bad Request', { status: 400 });
        }
      } else if (url.pathname === '/registerWebhook') {
        return await registerWebhook(request);
      } else if (url.pathname === '/unRegisterWebhook') {
        return await unRegisterWebhook();
      } else if (url.pathname === '/checkTables') {
        await checkAndRepairTables(env.D1);
        return new Response('Database tables checked and repaired', { status: 200 });
      }

      return new Response('Not Found', { status: 404 });
    }

    async function initialize(d1, request, env) {
      await Promise.all([
        checkAndRepairTables(d1),
        autoRegisterWebhook(request),
        checkAllBotPermissions(),
        cleanExpiredVerificationCodes(d1)
      ]);
    }

    // 为每个 bot 注册 webhook: origin/webhook?bot=index
    async function autoRegisterWebhook(request) {
      const origin = new URL(request.url).origin;
      for (let i = 0; i < BOT_TOKENS.length; i++) {
        const token = BOT_TOKENS[i];
        const webhookUrl = `${origin}/webhook?bot=${i}`;
        await fetchWithRetry(tgApiUrl(token, 'setWebhook'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl })
        });
      }
    }

    async function checkAllBotPermissions() {
      for (let i = 0; i < BOT_TOKENS.length; i++) {
        const token = BOT_TOKENS[i];
        await checkBotPermissions(token);
      }
    }

    async function checkBotPermissions(botToken) {
      const response = await fetchWithRetry(tgApiUrl(botToken, 'getChat'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID })
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to access group: ${data.description}`);
      }

      const meResp = await fetchWithRetry(tgApiUrl(botToken, 'getMe'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const meData = await meResp.json();
      if (!meData.ok) {
        throw new Error(`Failed to get bot ID: ${meData.description}`);
      }

      const botId = meData.result.id;

      const memberResponse = await fetchWithRetry(tgApiUrl(botToken, 'getChatMember'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: botId
        })
      });
      const memberData = await memberResponse.json();
      if (!memberData.ok) {
        throw new Error(`Failed to get bot member status: ${memberData.description}`);
      }
    }

    async function checkAndRepairTables(d1) {
      const expectedTables = {
        user_states: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY', // 存 userKey = `${botIndex}:${chatId}`
            is_blocked: 'BOOLEAN DEFAULT FALSE',
            is_verified: 'BOOLEAN DEFAULT FALSE',
            verified_expiry: 'INTEGER',
            verification_code: 'TEXT',
            code_expiry: 'INTEGER',
            last_verification_message_id: 'TEXT',
            is_first_verification: 'BOOLEAN DEFAULT TRUE',
            is_rate_limited: 'BOOLEAN DEFAULT FALSE',
            is_verifying: 'BOOLEAN DEFAULT FALSE'
          }
        },
        message_rates: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY', // 存 userKey
            message_count: 'INTEGER DEFAULT 0',
            window_start: 'INTEGER',
            start_count: 'INTEGER DEFAULT 0',
            start_window_start: 'INTEGER'
          }
        },
        chat_topic_mappings: {
          columns: {
            chat_id: 'TEXT PRIMARY KEY', // 存真实 chatId
            topic_id: 'TEXT NOT NULL'
          }
        },
        settings: {
          columns: {
            key: 'TEXT PRIMARY KEY',
            value: 'TEXT'
          }
        }
      };

      for (const [tableName, structure] of Object.entries(expectedTables)) {
        const tableInfo = await d1.prepare(
          `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
        ).bind(tableName).first();

        if (!tableInfo) {
          await createTable(d1, tableName, structure);
          continue;
        }

        const columnsResult = await d1.prepare(
          `PRAGMA table_info(${tableName})`
        ).all();

        const currentColumns = new Map(
          columnsResult.results.map(col => [col.name, {
            type: col.type,
            notnull: col.notnull,
            dflt_value: col.dflt_value
          }])
        );

        for (const [colName, colDef] of Object.entries(structure.columns)) {
          if (!currentColumns.has(colName)) {
            const columnParts = colDef.split(' ');
            const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(1).join(' ')}`;
            await d1.exec(addColumnSQL);
          }
        }

        if (tableName === 'settings') {
          await d1.exec('CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');
        }
      }

      await Promise.all([
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('verification_enabled', 'true').run(),
        d1.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)')
          .bind('user_raw_enabled', 'true').run()
      ]);

      settingsCache.set('verification_enabled', (await getSetting('verification_enabled', d1)) === 'true');
      settingsCache.set('user_raw_enabled', (await getSetting('user_raw_enabled', d1)) === 'true');
    }

    async function createTable(d1, tableName, structure) {
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `${name} ${def}`)
        .join(', ');
      const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
      await d1.exec(createSQL);
    }

    async function cleanExpiredVerificationCodes(d1) {
      const now = Date.now();
      if (now - lastCleanupTime < CLEANUP_INTERVAL) return;

      const nowSeconds = Math.floor(now / 1000);
      const expiredCodes = await d1.prepare(
        'SELECT chat_id FROM user_states WHERE code_expiry IS NOT NULL AND code_expiry < ?'
      ).bind(nowSeconds).all();

      if (expiredCodes.results.length > 0) {
        await d1.batch(
          expiredCodes.results.map(({ chat_id }) =>
            d1.prepare(
              'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
            ).bind(chat_id)
          )
        );
      }
      lastCleanupTime = now;
    }

    async function handleUpdate(update, botIndex, botToken, env) {
      if (update.message) {
        const messageId = update.message.message_id.toString();
        const chatId = update.message.chat.id.toString();
        const messageKey = `${botIndex}:${chatId}:${messageId}`;

        if (processedMessages.has(messageKey)) return;
        processedMessages.add(messageKey);
        if (processedMessages.size > 10000) processedMessages.clear();

        await onMessage(update.message, botIndex, botToken, env);
      } else if (update.callback_query) {
        const callbackKey = `${botIndex}:${update.callback_query.id}`;
        if (processedCallbacks.has(callbackKey)) return;
        processedCallbacks.add(callbackKey);
        if (processedCallbacks.size > 10000) processedCallbacks.clear();

        await onCallbackQuery(update.callback_query, botIndex, botToken, env);
      }
    }

    async function onMessage(message, botIndex, botToken, env) {
      const chatId = message.chat.id.toString();      // 真实用户 chatId 或群聊 id
      const userKey = makeUserKey(botIndex, chatId);  // 用于存状态 / 限流 / 黑名单
      const text = message.text || '';
      const messageId = message.message_id;

      // 群里的消息
      if (chatId === GROUP_ID.toString()) {
        const topicId = message.message_thread_id;
        if (topicId) {
          const privateChatId = await getPrivateChatId(topicId, env);
          if (privateChatId && text === '/admin') {
            await sendAdminPanel(chatId, topicId, privateChatId, messageId, botToken, env, botIndex);
            return;
          }
          if (privateChatId && text.startsWith('/reset_user')) {
            await handleResetUser(chatId, topicId, text, botIndex, env);
            return;
          }
          if (privateChatId) {
            await forwardMessageToPrivateChat(privateChatId, message, botToken);
          }
        }
        return;
      }

      // 私聊里的消息：按 bot 独立 userKey 管理
      let userState = userStateCache.get(userKey);
      if (userState === undefined) {
        userState = await env.D1.prepare(
          'SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying FROM user_states WHERE chat_id = ?'
        ).bind(userKey).first();
        if (!userState) {
          userState = {
            is_blocked: false,
            is_first_verification: true,
            is_verified: false,
            verified_expiry: null,
            is_verifying: false
          };
          await env.D1.prepare(
            'INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified, is_verifying) VALUES (?, ?, ?, ?, ?)'
          ).bind(userKey, false, true, false, false).run();
        }
        userStateCache.set(userKey, userState);
      }

      if (userState.is_blocked) {
        await sendMessageToUser(chatId, '您已被拉黑，无法发送消息。请联系管理员解除拉黑。', botToken);
        return;
      }

      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';

      if (verificationEnabled) {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const isVerified =
          userState.is_verified &&
          userState.verified_expiry &&
          nowSeconds < userState.verified_expiry;
        const isFirstVerification = userState.is_first_verification;
        const isRateLimited = await checkMessageRate(userKey, env);
        const isVerifying = userState.is_verifying || false;

        if (!isVerified || (isRateLimited && !isFirstVerification)) {
          if (isVerifying) {
            const storedCode = await env.D1.prepare(
              'SELECT verification_code, code_expiry FROM user_states WHERE chat_id = ?'
            ).bind(userKey).first();

            const nowSeconds2 = Math.floor(Date.now() / 1000);
            const isCodeExpired =
              !storedCode?.verification_code ||
              !storedCode?.code_expiry ||
              nowSeconds2 > storedCode.code_expiry;

            if (isCodeExpired) {
              await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...', botToken);
              await env.D1.prepare(
                'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
              ).bind(userKey).run();
              userStateCache.set(userKey, {
                ...userState,
                verification_code: null,
                code_expiry: null,
                is_verifying: false
              });

              // 删除旧验证消息
              try {
                const lastVerification = await env.D1.prepare(
                  'SELECT last_verification_message_id FROM user_states WHERE chat_id = ?'
                ).bind(userKey).first();

                if (lastVerification?.last_verification_message_id) {
                  try {
                    await fetchWithRetry(tgApiUrl(botToken, 'deleteMessage'), {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        chat_id: chatId,
                        message_id: lastVerification.last_verification_message_id
                      })
                    });
                  } catch (e) {
                    console.log(`删除旧验证消息失败: ${e.message}`);
                  }

                  await env.D1.prepare(
                    'UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?'
                  ).bind(userKey).run();
                }
              } catch (e) {
                console.log(`查询旧验证消息失败: ${e.message}`);
              }

              try {
                await handleVerification(chatId, userKey, 0, botToken, env);
              } catch (err) {
                console.error(`发送新验证码失败: ${err.message}`);
                setTimeout(async () => {
                  try {
                    await handleVerification(chatId, userKey, 0, botToken, env);
                  } catch (retryErr) {
                    console.error(`重试发送验证码仍失败: ${retryErr.message}`);
                    await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试', botToken);
                  }
                }, 1000);
              }
              return;
            } else {
              await sendMessageToUser(
                chatId,
                `请完成验证后发送消息"${text || '您的具体信息'}"。`,
                botToken
              );
            }
            return;
          }

          await sendMessageToUser(
            chatId,
            `请完成验证后发送消息"${text || '您的具体信息'}"。`,
            botToken
          );
          await handleVerification(chatId, userKey, messageId, botToken, env);
          return;
        }
      }

      if (text === '/start') {
        if (await checkStartCommandRate(userKey, env)) {
          await sendMessageToUser(chatId, '您发送 /start 命令过于频繁，请稍后再试！', botToken);
          return;
        }

        const successMessage = await getVerificationSuccessMessage(env);
        await sendMessageToUser(
          chatId,
          `${successMessage}\n你好，欢迎使用私聊机器人，现在发送信息吧！`,
          botToken
        );
        const userInfo = await getUserInfo(chatId, botToken);
        await ensureUserTopic(chatId, userInfo, env, botToken);
        return;
      }

      const userInfo = await getUserInfo(chatId, botToken);
      if (!userInfo) {
        await sendMessageToUser(chatId, '无法获取用户信息，请稍后再试或联系管理员。', botToken);
        return;
      }

      let topicId = await ensureUserTopic(chatId, userInfo, env, botToken);
      if (!topicId) {
        await sendMessageToUser(chatId, '无法创建话题，请稍后再试或联系管理员。', botToken);
        return;
      }

      const isTopicValid = await validateTopic(topicId, botToken);
      if (!isTopicValid) {
        await env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?')
          .bind(chatId)
          .run();
        topicIdCache.set(chatId, undefined);

        topicId = await ensureUserTopic(chatId, userInfo, env, botToken);
        if (!topicId) {
          await sendMessageToUser(chatId, '无法重新创建话题，请稍后再试或联系管理员。', botToken);
          return;
        }
      }

      const userName = userInfo.username || `User_${chatId}`;
      const nickname = userInfo.nickname || userName;

      if (text) {
        const formattedMessage = `${nickname}:\n${text}`;
        await sendMessageToTopic(topicId, formattedMessage, botToken);
      } else {
        await copyMessageToTopic(topicId, message, botToken);
      }
    }

    async function validateTopic(topicId, botToken) {
      try {
        const response = await fetchWithRetry(tgApiUrl(botToken, 'sendMessage'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            text: '您有新消息！',
            disable_notification: true
          })
        });
        const data = await response.json();
        if (data.ok) {
          await fetchWithRetry(tgApiUrl(botToken, 'deleteMessage'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: GROUP_ID,
              message_id: data.result.message_id
            })
          });
          return true;
        }
        return false;
      } catch {
        return false;
      }
    }

    async function ensureUserTopic(chatId, userInfo, env, botToken) {
      let lock = topicCreationLocks.get(chatId);
      if (!lock) {
        lock = Promise.resolve();
        topicCreationLocks.set(chatId, lock);
      }

      try {
        await lock;

        let topicId = await getExistingTopicId(chatId, env);
        if (topicId) return topicId;

        const newLock = (async () => {
          const userName = userInfo.username || `User_${chatId}`;
          const nickname = userInfo.nickname || userName;
          const newTopicId = await createForumTopic(nickname, userName, nickname, userInfo.id || chatId, env, botToken);
          await saveTopicId(chatId, newTopicId, env);
          return newTopicId;
        })();

        topicCreationLocks.set(chatId, newLock);
        return await newLock;
      } finally {
        if (topicCreationLocks.get(chatId) === lock) {
          topicCreationLocks.delete(chatId);
        }
      }
    }

    async function handleResetUser(chatId, topicId, text, botIndex, env) {
      const senderId = chatId;
      // 管理员权限检查：任意 bot 的管理员即可
      // 这里用第一个 bot 的 token 来查权限
      const isAdmin = await checkIfAdmin(senderId, BOT_TOKENS[0]);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此功能。', BOT_TOKENS[0]);
        return;
      }

      const parts = text.split(' ');
      if (parts.length !== 2) {
        await sendMessageToTopic(topicId, '用法：/reset_user <chat_id>', BOT_TOKENS[0]);
        return;
      }

      const targetChatId = parts[1];
      const userKey = makeUserKey(botIndex, targetChatId);

      await env.D1.batch([
        env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(userKey),
        env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(userKey),
        env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(targetChatId)
      ]);

      userStateCache.set(userKey, undefined);
      messageRateCache.set(userKey, undefined);
      topicIdCache.set(targetChatId, undefined);

      await sendMessageToTopic(
        topicId,
        `用户 ${targetChatId} 在当前 bot 的状态已重置（话题也已重置）。`,
        BOT_TOKENS[0]
      );
    }

    async function sendAdminPanel(chatId, topicId, privateChatId, messageId, botToken, env, botIndex) {
      const verificationEnabled = (await getSetting('verification_enabled', env.D1)) === 'true';
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';

      const buttons = [
        [
          { text: '拉黑用户', callback_data: `block_${privateChatId}` },
          { text: '解除拉黑', callback_data: `unblock_${privateChatId}` }
        ],
        [
          {
            text: verificationEnabled ? '关闭验证码' : '开启验证码',
            callback_data: `toggle_verification_${privateChatId}`
          },
          { text: '查询黑名单', callback_data: `check_blocklist_${privateChatId}` }
        ],
        [
          {
            text: userRawEnabled ? '关闭用户Raw' : '开启用户Raw',
            callback_data: `toggle_user_raw_${privateChatId}`
          }
        ],
        [{ text: '删除用户', callback_data: `delete_user_${privateChatId}` }]
      ];

      const adminMessage = '管理员面板：请选择操作';
      await Promise.all([
        fetchWithRetry(tgApiUrl(botToken, 'sendMessage'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_thread_id: topicId,
            text: adminMessage,
            reply_markup: { inline_keyboard: buttons }
          })
        }),
        fetchWithRetry(tgApiUrl(botToken, 'deleteMessage'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          })
        })
      ]);
    }

    async function getVerificationSuccessMessage(env) {
      const userRawEnabled = (await getSetting('user_raw_enabled', env.D1)) === 'true';
      const defaultMsg = '验证成功！您现在可以与我聊天。';

      if (!userRawEnabled) return defaultMsg;

      const customMsg = env.VERIFICATION_SUCCESS_MESSAGE || '';
      const text = (customMsg && customMsg.trim()) || defaultMsg;
      return text;
    }

    async function getNotificationContent(env) {
      const content = env.NOTIFICATION_CONTENT || '';
      return (content && content.trim()) || '';
    }

    async function checkStartCommandRate(userKey, env) {
      const now = Date.now();
      const window = 5 * 60 * 1000;
      const maxStartsPerWindow = 1;

      let data = messageRateCache.get(userKey);
      if (data === undefined) {
        data = await env.D1.prepare(
          'SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?'
        ).bind(userKey).first();
        if (!data) {
          data = { start_count: 0, start_window_start: now };
          await env.D1.prepare(
            'INSERT INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)'
          ).bind(userKey, data.start_count, data.start_window_start).run();
        }
        messageRateCache.set(userKey, data);
      }

      if (now - data.start_window_start > window) {
        data.start_count = 1;
        data.start_window_start = now;
        await env.D1.prepare(
          'UPDATE message_rates SET start_count = ?, start_window_start = ? WHERE chat_id = ?'
        ).bind(data.start_count, data.start_window_start, userKey).run();
      } else {
        data.start_count += 1;
        await env.D1.prepare(
          'UPDATE message_rates SET start_count = ? WHERE chat_id = ?'
        ).bind(data.start_count, userKey).run();
      }

      messageRateCache.set(userKey, data);
      return data.start_count > maxStartsPerWindow;
    }

    async function checkMessageRate(userKey, env) {
      const now = Date.now();
      const window = 60 * 1000;

      let data = messageRateCache.get(userKey);
      if (data === undefined) {
        data = await env.D1.prepare(
          'SELECT message_count, window_start FROM message_rates WHERE chat_id = ?'
        ).bind(userKey).first();
        if (!data) {
          data = { message_count: 0, window_start: now };
          await env.D1.prepare(
            'INSERT INTO message_rates (chat_id, message_count, window_start) VALUES (?, ?, ?)'
          ).bind(userKey, data.message_count, data.window_start).run();
        }
        messageRateCache.set(userKey, data);
      }

      if (now - data.window_start > window) {
        data.message_count = 1;
        data.window_start = now;
      } else {
        data.message_count += 1;
      }

      messageRateCache.set(userKey, data);
      await env.D1.prepare(
        'UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?'
      ).bind(data.message_count, data.window_start, userKey).run();
      return data.message_count > MAX_MESSAGES_PER_MINUTE;
    }

    async function getSetting(key, d1) {
      const cached = settingsCache.get(key);
      if (cached !== null && cached !== undefined) return cached;

      const result = await d1.prepare('SELECT value FROM settings WHERE key = ?')
        .bind(key)
        .first();
      const value = result?.value || null;
      settingsCache.set(key, value);
      return value;
    }

    async function setSetting(key, value, env) {
      await env.D1.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)')
        .bind(key, value)
        .run();
      if (key === 'verification_enabled') {
        settingsCache.set('verification_enabled', value === 'true');
        if (value === 'false') {
          const nowSeconds = Math.floor(Date.now() / 1000);
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare(
            'UPDATE user_states SET is_verified = ?, verified_expiry = ?, is_verifying = ?, verification_code = NULL, code_expiry = NULL, is_first_verification = ? WHERE chat_id NOT IN (SELECT chat_id FROM user_states WHERE is_blocked = TRUE)'
          ).bind(true, verifiedExpiry, false, false).run();
          userStateCache.clear();
        }
      } else if (key === 'user_raw_enabled') {
        settingsCache.set('user_raw_enabled', value === 'true');
      }
    }

    async function onCallbackQuery(callbackQuery, botIndex, botToken, env) {
      const chatId = callbackQuery.message.chat.id.toString();
      const topicId = callbackQuery.message.message_thread_id;
      const data = callbackQuery.data;
      const messageId = callbackQuery.message.message_id;
      const userKey = makeUserKey(botIndex, chatId);

      const parts = data.split('_');
      let action;
      let privateChatId;

      if (data.startsWith('verify_')) {
        action = 'verify';
        privateChatId = parts[1];
      } else if (data.startsWith('toggle_verification_')) {
        action = 'toggle_verification';
        privateChatId = parts.slice(2).join('_');
      } else if (data.startsWith('toggle_user_raw_')) {
        action = 'toggle_user_raw';
        privateChatId = parts.slice(3).join('_');
      } else if (data.startsWith('check_blocklist_')) {
        action = 'check_blocklist';
        privateChatId = parts.slice(2).join('_');
      } else if (data.startsWith('block_')) {
        action = 'block';
        privateChatId = parts.slice(1).join('_');
      } else if (data.startsWith('unblock_')) {
        action = 'unblock';
        privateChatId = parts.slice(1).join('_');
      } else if (data.startsWith('delete_user_')) {
        action = 'delete_user';
        privateChatId = parts.slice(2).join('_');
      } else {
        action = data;
        privateChatId = '';
      }

      if (action === 'verify') {
        const [, userChatId, selectedAnswer, result] = data.split('_');
        if (userChatId !== chatId) {
          // 只处理自己这条验证
          await answerCallback(callbackQuery.id, botToken);
          return;
        }

        let verificationState = userStateCache.get(userKey);
        if (verificationState === undefined) {
          verificationState = await env.D1.prepare(
            'SELECT verification_code, code_expiry, is_verifying FROM user_states WHERE chat_id = ?'
          ).bind(userKey).first();
          if (!verificationState) {
            verificationState = { verification_code: null, code_expiry: null, is_verifying: false };
          }
          userStateCache.set(userKey, verificationState);
        }

        const storedCode = verificationState.verification_code;
        const codeExpiry = verificationState.code_expiry;
        const nowSeconds = Math.floor(Date.now() / 1000);

        if (!storedCode || (codeExpiry && nowSeconds > codeExpiry)) {
          await sendMessageToUser(chatId, '验证码已过期，正在为您发送新的验证码...', botToken);
          await env.D1.prepare(
            'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?'
          ).bind(userKey).run();
          userStateCache.set(userKey, {
            ...verificationState,
            verification_code: null,
            code_expiry: null,
            is_verifying: false
          });

          try {
            await fetchWithRetry(tgApiUrl(botToken, 'deleteMessage'), {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: messageId
              })
            });
          } catch (e) {
            console.log(`删除过期验证按钮失败: ${e.message}`);
          }

          try {
            await handleVerification(chatId, userKey, 0, botToken, env);
          } catch (err) {
            console.error(`发送新验证码失败: ${err.message}`);
            setTimeout(async () => {
              try {
                await handleVerification(chatId, userKey, 0, botToken, env);
              } catch (retryErr) {
                console.error(`重试发送验证码仍失败: ${retryErr.message}`);
                await sendMessageToUser(chatId, '发送验证码失败，请发送任意消息重试', botToken);
              }
            }, 1000);
          }
          await answerCallback(callbackQuery.id, botToken);
          return;
        }

        if (result === 'correct') {
          const verifiedExpiry = nowSeconds + 3600 * 24;
          await env.D1.prepare(
            'UPDATE user_states SET is_verified = ?, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, last_verification_message_id = NULL, is_first_verification = ?, is_verifying = ? WHERE chat_id = ?'
          ).bind(true, verifiedExpiry, false, false, userKey).run();

          verificationState = await env.D1.prepare(
            'SELECT is_verified, verified_expiry, verification_code, code_expiry, last_verification_message_id, is_first_verification, is_verifying FROM user_states WHERE chat_id = ?'
          ).bind(userKey).first();
          userStateCache.set(userKey, verificationState);

          let rateData = await env.D1.prepare(
            'SELECT message_count, window_start FROM message_rates WHERE chat_id = ?'
          ).bind(userKey).first() || { message_count: 0, window_start: nowSeconds * 1000 };
          rateData.message_count = 0;
          rateData.window_start = nowSeconds * 1000;
          messageRateCache.set(userKey, rateData);
          await env.D1.prepare(
            'UPDATE message_rates SET message_count = ?, window_start = ? WHERE chat_id = ?'
          ).bind(0, nowSeconds * 1000, userKey).run();

          const successMessage = await getVerificationSuccessMessage(env);
          await sendMessageToUser(
            chatId,
            `${successMessage}\n你好，欢迎使用私聊机器人！现在可以发送消息了。`,
            botToken
          );
          const userInfo = await getUserInfo(chatId, botToken);
          await ensureUserTopic(chatId, userInfo, env, botToken);
        } else {
          await sendMessageToUser(chatId, '验证失败，请重新尝试。', botToken);
          await handleVerification(chatId, userKey, messageId, botToken, env);
        }

        await fetchWithRetry(tgApiUrl(botToken, 'deleteMessage'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          })
        });

        await answerCallback(callbackQuery.id, botToken);
        return;
      }

      // 下面是管理员相关操作
      const senderId = callbackQuery.from.id.toString();
      const isAdmin = await checkIfAdmin(senderId, botToken);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此功能。', botToken);
        await sendAdminPanel(chatId, topicId, privateChatId, messageId, botToken, env, botIndex);
        await answerCallback(callbackQuery.id, botToken);
        return;
      }

      const targetChatId = privateChatId;
      const targetUserKey = makeUserKey(botIndex, targetChatId);

      if (action === 'block') {
        let state = userStateCache.get(targetUserKey);
        if (state === undefined) {
          state = await env.D1.prepare(
            'SELECT is_blocked FROM user_states WHERE chat_id = ?'
          ).bind(targetUserKey).first() || { is_blocked: false };
        }
        state.is_blocked = true;
        userStateCache.set(targetUserKey, state);
        await env.D1.prepare(
          'INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)'
        ).bind(targetUserKey, true).run();
        await sendMessageToTopic(
          topicId,
          `用户 ${targetChatId} 已在当前 bot 被拉黑，消息将不再转发。`,
          botToken
        );
      } else if (action === 'unblock') {
        let state = userStateCache.get(targetUserKey);
        if (state === undefined) {
          state = await env.D1.prepare(
            'SELECT is_blocked, is_first_verification FROM user_states WHERE chat_id = ?'
          ).bind(targetUserKey).first() || { is_blocked: false, is_first_verification: true };
        }
        state.is_blocked = false;
        state.is_first_verification = true;
        userStateCache.set(targetUserKey, state);
        await env.D1.prepare(
          'INSERT OR REPLACE INTO user_states (chat_id, is_blocked, is_first_verification) VALUES (?, ?, ?)'
        ).bind(targetUserKey, false, true).run();
        await sendMessageToTopic(
          topicId,
          `用户 ${targetChatId} 已在当前 bot 解除拉黑，消息将继续转发。`,
          botToken
        );
      } else if (action === 'toggle_verification') {
        const currentState = (await getSetting('verification_enabled', env.D1)) === 'true';
        const newState = !currentState;
        await setSetting('verification_enabled', newState.toString(), env);
        await sendMessageToTopic(topicId, `验证码功能已${newState ? '开启' : '关闭'}。`, botToken);
      } else if (action === 'check_blocklist') {
        const prefix = `${botIndex}:`;
        const blockedUsers = await env.D1.prepare(
          'SELECT chat_id FROM user_states WHERE is_blocked = ? AND chat_id LIKE ?'
        ).bind(true, `${prefix}%`).all();

        const list = blockedUsers.results.map(row => row.chat_id.replace(prefix, ''));
        const blockList = list.length > 0 ? list.join('\n') : '当前没有被拉黑的用户。';
        await sendMessageToTopic(
          topicId,
          `黑名单列表（当前 bot）：\n${blockList}`,
          botToken
        );
      } else if (action === 'toggle_user_raw') {
        const currentState = (await getSetting('user_raw_enabled', env.D1)) === 'true';
        const newState = !currentState;
        await setSetting('user_raw_enabled', newState.toString(), env);
        await sendMessageToTopic(
          topicId,
          `用户端 Raw 链接已${newState ? '开启' : '关闭'}。`,
          botToken
        );
      } else if (action === 'delete_user') {
        userStateCache.set(targetUserKey, undefined);
        messageRateCache.set(targetUserKey, undefined);
        topicIdCache.set(targetChatId, undefined);
        await env.D1.batch([
          env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(targetUserKey),
          env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(targetUserKey),
          env.D1.prepare('DELETE FROM chat_topic_mappings WHERE chat_id = ?').bind(targetChatId)
        ]);
        await sendMessageToTopic(
          topicId,
          `用户 ${targetChatId} 在当前 bot 的状态、消息记录和话题映射已删除，用户需重新发起会话。`,
          botToken
        );
      } else {
        await sendMessageToTopic(topicId, `未知操作：${action}`, botToken);
      }

      await sendAdminPanel(chatId, topicId, privateChatId, messageId, botToken, env, botIndex);
      await answerCallback(callbackQuery.id, botToken);
    }

    async function answerCallback(callbackQueryId, botToken) {
      await fetchWithRetry(tgApiUrl(botToken, 'answerCallbackQuery'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: callbackQueryId })
      });
    }

    async function handleVerification(chatId, userKey, messageId, botToken, env) {
      try {
        let userState = userStateCache.get(userKey);
        if (userState === undefined) {
          userState = await env.D1.prepare(
            'SELECT is_blocked, is_first_verification, is_verified, verified_expiry, is_verifying FROM user_states WHERE chat_id = ?'
          ).bind(userKey).first();
          if (!userState) {
            userState = {
              is_blocked: false,
              is_first_verification: true,
              is_verified: false,
              verified_expiry: null,
              is_verifying: false
            };
          }
          userStateCache.set(userKey, userState);
        }

        userState.verification_code = null;
        userState.code_expiry = null;
        userState.is_verifying = true;
        userStateCache.set(userKey, userState);
        await env.D1.prepare(
          'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = ? WHERE chat_id = ?'
        ).bind(true, userKey).run();

        const lastVerification =
          userState.last_verification_message_id ||
          (await env.D1.prepare(
            'SELECT last_verification_message_id FROM user_states WHERE chat_id = ?'
          ).bind(userKey).first())?.last_verification_message_id;

        if (lastVerification) {
          try {
            await fetchWithRetry(tgApiUrl(botToken, 'deleteMessage'), {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: lastVerification
              })
            });
          } catch (e) {
            console.log(`删除上一条验证消息失败: ${e.message}`);
          }

          userState.last_verification_message_id = null;
          userStateCache.set(userKey, userState);
          await env.D1.prepare(
            'UPDATE user_states SET last_verification_message_id = NULL WHERE chat_id = ?'
          ).bind(userKey).run();
        }

        await sendVerification(chatId, userKey, botToken, env);
      } catch (error) {
        console.error(`处理验证过程失败: ${error.message}`);
        try {
          await env.D1.prepare('UPDATE user_states SET is_verifying = FALSE WHERE chat_id = ?')
            .bind(userKey)
            .run();
          const currentState = userStateCache.get(userKey);
          if (currentState) {
            currentState.is_verifying = false;
            userStateCache.set(userKey, currentState);
          }
        } catch (resetError) {
          console.error(`重置用户验证状态失败: ${resetError.message}`);
        }
        throw error;
      }
    }

    async function sendVerification(chatId, userKey, botToken, env) {
      try {
        const num1 = Math.floor(Math.random() * 10);
        const num2 = Math.floor(Math.random() * 10);
        const operation = Math.random() > 0.5 ? '+' : '-';
        const correctResult = operation === '+' ? num1 + num2 : num1 - num2;

        const options = new Set([correctResult]);
        while (options.size < 4) {
          const wrongResult = correctResult + Math.floor(Math.random() * 5) - 2;
          if (wrongResult !== correctResult) options.add(wrongResult);
        }
        const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

        const buttons = optionArray.map(option => ({
          text: `(${option})`,
          callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`
        }));

        const question = `请计算：${num1} ${operation} ${num2} = ?（点击下方按钮完成验证）`;
        const nowSeconds = Math.floor(Date.now() / 1000);
        const codeExpiry = nowSeconds + 300;

        let userState = userStateCache.get(userKey);
        if (userState === undefined) {
          userState = {
            verification_code: correctResult.toString(),
            code_expiry: codeExpiry,
            last_verification_message_id: null,
            is_verifying: true
          };
        } else {
          userState.verification_code = correctResult.toString();
          userState.code_expiry = codeExpiry;
          userState.last_verification_message_id = null;
          userState.is_verifying = true;
        }
        userStateCache.set(userKey, userState);

        const response = await fetchWithRetry(tgApiUrl(botToken, 'sendMessage'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: question,
            reply_markup: { inline_keyboard: [buttons] }
          })
        });
        const data = await response.json();
        if (data.ok) {
          userState.last_verification_message_id = data.result.message_id.toString();
          userStateCache.set(userKey, userState);
          await env.D1.prepare(
            'UPDATE user_states SET verification_code = ?, code_expiry = ?, last_verification_message_id = ?, is_verifying = ? WHERE chat_id = ?'
          ).bind(
            correctResult.toString(),
            codeExpiry,
            data.result.message_id.toString(),
            true,
            userKey
          ).run();
        } else {
          throw new Error(`Telegram API 返回错误: ${data.description || '未知错误'}`);
        }
      } catch (error) {
        console.error(`发送验证码失败: ${error.message}`);
        throw error;
      }
    }

    async function checkIfAdmin(userId, botToken) {
      const response = await fetchWithRetry(tgApiUrl(botToken, 'getChatMember'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: GROUP_ID,
          user_id: userId
        })
      });
      const data = await response.json();
      return (
        data.ok &&
        (data.result.status === 'administrator' || data.result.status === 'creator')
      );
    }

    async function getUserInfo(chatId, botToken) {
      let userInfo = userInfoCache.get(chatId);
      if (userInfo !== undefined) return userInfo;

      const response = await fetchWithRetry(tgApiUrl(botToken, 'getChat'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId })
      });
      const data = await response.json();
      if (!data.ok) {
        userInfo = {
          id: chatId,
          username: `User_${chatId}`,
          nickname: `User_${chatId}`
        };
      } else {
        const result = data.result;
        const nickname = result.first_name
          ? `${result.first_name}${result.last_name ? ` ${result.last_name}` : ''}`.trim()
          : result.username || `User_${chatId}`;
        userInfo = {
          id: result.id || chatId,
          username: result.username || `User_${chatId}`,
          nickname
        };
      }

      userInfoCache.set(chatId, userInfo);
      return userInfo;
    }

    async function getExistingTopicId(chatId, env) {
      let topicId = topicIdCache.get(chatId);
      if (topicId !== undefined) return topicId;

      const result = await env.D1.prepare(
        'SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?'
      ).bind(chatId).first();
      topicId = result?.topic_id || null;
      if (topicId) topicIdCache.set(chatId, topicId);
      return topicId;
    }

    async function createForumTopic(topicName, userName, nickname, userId, env, botToken) {
      const response = await fetchWithRetry(tgApiUrl(botToken, 'createForumTopic'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, name: `${nickname}` })
      });
      const data = await response.json();
      if (!data.ok) throw new Error(`Failed to create forum topic: ${data.description}`);
      const topicId = data.result.message_thread_id;

      const now = new Date();
      const formattedTime = now.toISOString().replace('T', ' ').substring(0, 19);
      const notificationContent = await getNotificationContent(env);
      const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: ${userId}\n发起时间: ${formattedTime}\n\n${notificationContent}`;
      const messageResponse = await sendMessageToTopic(topicId, pinnedMessage, botToken);
      const messageId = messageResponse.result.message_id;
      await pinMessage(topicId, messageId, botToken);

      return topicId;
    }

    async function saveTopicId(chatId, topicId, env) {
      await env.D1.prepare(
        'INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)'
      ).bind(chatId, topicId).run();
      topicIdCache.set(chatId, topicId);
    }

    async function getPrivateChatId(topicId, env) {
      for (const [chatId, tid] of topicIdCache.cache) {
        if (tid === topicId) return chatId;
      }
      const mapping = await env.D1.prepare(
        'SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?'
      ).bind(topicId).first();
      return mapping?.chat_id || null;
    }

    async function sendMessageToTopic(topicId, text, botToken) {
      if (!text.trim()) throw new Error('Message text is empty');

      const requestBody = {
        chat_id: GROUP_ID,
        text,
        message_thread_id: topicId
      };
      const response = await fetchWithRetry(tgApiUrl(botToken, 'sendMessage'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to topic ${topicId}: ${data.description}`);
      }
      return data;
    }

    async function copyMessageToTopic(topicId, message, botToken) {
      const requestBody = {
        chat_id: GROUP_ID,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        message_thread_id: topicId,
        disable_notification: true
      };
      const response = await fetchWithRetry(tgApiUrl(botToken, 'copyMessage'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to copy message to topic ${topicId}: ${data.description}`);
      }
    }

    async function pinMessage(topicId, messageId, botToken) {
      const requestBody = {
        chat_id: GROUP_ID,
        message_id: messageId,
        message_thread_id: topicId
      };
      const response = await fetchWithRetry(tgApiUrl(botToken, 'pinChatMessage'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to pin message: ${data.description}`);
      }
    }

    async function forwardMessageToPrivateChat(privateChatId, message, botToken) {
      const requestBody = {
        chat_id: privateChatId,
        from_chat_id: message.chat.id,
        message_id: message.message_id,
        disable_notification: true
      };
      const response = await fetchWithRetry(tgApiUrl(botToken, 'copyMessage'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to forward message to private chat: ${data.description}`);
      }
    }

    async function sendMessageToUser(chatId, text, botToken) {
      const requestBody = { chat_id: chatId, text };
      const response = await fetchWithRetry(tgApiUrl(botToken, 'sendMessage'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to send message to user: ${data.description}`);
      }
    }

    async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
      for (let i = 0; i < retries; i++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 5000);
          const response = await fetch(url, { ...options, signal: controller.signal });
          clearTimeout(timeoutId);

          if (response.ok) return response;
          if (response.status === 429) {
            const retryAfter = response.headers.get('Retry-After') || 5;
            const delay = parseInt(retryAfter, 10) * 1000;
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          throw new Error(
            `Request failed with status ${response.status}: ${await response.text()}`
          );
        } catch (error) {
          if (i === retries - 1) throw error;
          await new Promise(resolve => setTimeout(resolve, backoff * Math.pow(2, i)));
        }
      }
      throw new Error(`Failed to fetch ${url} after ${retries} retries`);
    }

    // 手动重新注册所有 bot 的 webhook
    async function registerWebhook(request) {
      const origin = new URL(request.url).origin;
      const results = [];

      for (let i = 0; i < BOT_TOKENS.length; i++) {
        const token = BOT_TOKENS[i];
        const webhookUrl = `${origin}/webhook?bot=${i}`;
        const resp = await fetch(tgApiUrl(token, 'setWebhook'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl })
        }).then(r => r.json());
        results.push({ index: i, ok: resp.ok, description: resp.description });
      }

      return new Response(JSON.stringify(results, null, 2), { status: 200 });
    }

    // 取消所有 bot 的 webhook
    async function unRegisterWebhook() {
      const results = [];
      for (const token of BOT_TOKENS) {
        const resp = await fetch(tgApiUrl(token, 'setWebhook'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: '' })
        }).then(r => r.json());
        results.push({ ok: resp.ok, description: resp.description });
      }
      return new Response(JSON.stringify(results, null, 2), { status: 200 });
    }

    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Internal error:', error);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
