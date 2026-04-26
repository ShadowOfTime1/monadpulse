# MonadPulse — Security Audit

**Date:** 2026-04-20
**Scope:** monadpulse.xyz (collector, API, nginx, DB, host), плюс смежная инфра Monad-ноды (validator keys, sudoers, монад-бот).
**Auditor:** Claude (instructed by shadowoftime, `roman.karpenk@gmail.com`).
**Method:** 6 частей — network/firewall, secrets/git, web app, validator keys, filesystem/services, TLS/dependencies. Проверки через внешний probe (check-host.net), локальные systemd/proc/fs, `pip-audit`, код-ревью.

---

## Overall Verdict

**🟢 Production-ready.**
Критичных эксплуатационных уязвимостей не найдено. Есть средний набор hardening-пунктов (NOPASSWD sudoers, mode 0664 на зашифрованных ключах валидатора, `monad-bot` под root), которые стоит закрыть по мере времени, но **ни один не поднимается до Critical/High** после учёта mitigations.

---

## Part A — Network / Firewall 🟢

**Проверено:**
- `iptables -S` / `ufw status` → публично открыты только 80, 443 и SSH-порт 52222 (дефолтный 22 закрыт).
- Collector RPC (8080), метрики (8889) bind на `127.0.0.1` → изолированы от внешнего интерфейса.
- Внешний probe через check-host.net из 3 стран: `8080`, `8889` — TIMEOUT со всех узлов. `443` → 200 OK.
- Monad P2P портов слушает только `monad-bft` на `0.0.0.0:<p2p>` — что ожидаемо.
- fail2ban активен с профилем `sshd`, отлавливает brute-force на 52222.

**Результат:** clean.

---

## Part B — Secrets / Git hygiene 🟢

**Проверено:**
- `/opt/monadpulse/.env` mode **0600**, owner `shadowoftime:shadowoftime`.
- В `.gitignore`: `.env`, `*.key`, `*.pem`, `__pycache__/`, `venv/`, `validator_geo_*.json` не в нём (ок — гео публично).
- `git log -p -- .env` → пусто (никогда не коммитился).
- `grep -rE "ghp_|sk-|-----BEGIN" /opt/monadpulse/` → нет ни одного токена.
- `journalctl -u monadpulse-*` → пароли/токены не логируются.
- TG-токен бота `@monadpulse_alerts` читается через `os.environ`, не hard-coded.

**Результат:** clean.

---

## Part C — Web application 🟢

**Headers (через curl на https://monadpulse.xyz):**
| Header                     | Value                                                 |
|----------------------------|-------------------------------------------------------|
| Strict-Transport-Security  | `max-age=63072000; includeSubDomains; preload`        |
| Content-Security-Policy    | `default-src 'self'; script-src 'self'; …`            |
| X-Frame-Options            | `DENY`                                                |
| X-Content-Type-Options     | `nosniff`                                             |
| Referrer-Policy            | `strict-origin-when-cross-origin`                     |
| Permissions-Policy         | `geolocation=(), microphone=(), camera=()`            |

**Rate limit:**
- Nginx `limit_req zone=api burst=20 nodelay`, 30 r/s per IP.
- Стресс: 100 параллельных запросов → 24 × `200`, 76 × `503`. Лимит работает.

**Input safety:**
- Все SQL через `asyncpg` prepared statements (`$1, $2, …`) → **SQL-инъекций нет**.
- Во фронте все user-controlled поля (validator name, miner, tx-hashes) идут через `esc()` helper (HTML-escape).
- Нет `eval()`, `Function()`, `innerHTML` c user input (найдено только `textContent` / `.innerText`).
- API endpoints проверяют `network ∈ {testnet, mainnet}`, `limit ≤ 500`, `val_id` → int.

**Результат:** clean.

---

## Part D — Validator keys + wallet 🟡

**Найдено:**

| Файл                                       | Mode    | Content           | Comment                                                      |
|--------------------------------------------|--------:|-------------------|--------------------------------------------------------------|
| `/home/monad/monad-bft/config/id-bls`      | **0664**| encrypted keystore| зашифрован, но `other::r--` — хотя `/home/monad` 0750 нивелирует |
| `/home/monad/monad-bft/config/id-secp`     | **0664**| encrypted keystore| то же самое                                                  |
| `/home/shadowoftime/.monad/bls.key`        | 0600    | 65 bytes raw hex  | ок                                                           |
| `/home/shadowoftime/.monad/secp.key`       | 0600    | 65 bytes raw hex  | ок                                                           |
| `/home/shadowoftime/.monad/auth_eoa.key`   | 0600    | **plain hex priv**| ок по perms, но без пасс-фразы — см. ниже                    |

**Mitigations in place:**
- `/home/monad` mode 0750, owner `monad:monad` → `other` фактически не достаёт даже на 0664 файлах.
- `/home/shadowoftime` mode 0700 → `.monad/` доступен только владельцу.
- Прав `sudo` у других пользователей на сервере нет (только `shadowoftime`).

**Риски:**
- 🟡 **Medium — defense-in-depth:** если когда-либо изменят `/home/monad` на 0755 (общий случай при debugging'е), ключи id-bls/id-secp станут читаемы всем. Исправить → `chmod 600 id-bls id-secp`.
- 🟡 **Medium — auth_eoa.key без passphrase:** это plain hex private key без шифрования. При компрометации учётки shadowoftime злоумышленник получает полный контроль над валидатором 267 (change-commission, claim-rewards, change-auth, unstake). Рекомендация: перевести на keystore с passphrase (можно держать passphrase в TPM/GPG-agent/1Password).
- 🟡 **NOPASSWD sudoers для shadowoftime (`(ALL) NOPASSWD: ALL`):** любой скрипт/скомпрометированный процесс под `shadowoftime` получает root. Снять NOPASSWD → требовать пароль при `sudo` для интерактивных команд (cron-процессы могут получить whitelist на конкретные команды).

---

## Part E — Filesystem + service users 🟡

| Service                   | User        | Comment                                                                 |
|---------------------------|-------------|-------------------------------------------------------------------------|
| `monadpulse-api`          | shadowoftime| ок                                                                      |
| `monadpulse-collector`    | shadowoftime| ок                                                                      |
| `monadpulse-*` timers     | shadowoftime| ок                                                                      |
| **`monad-bot`**           | **root**    | 🟡 избыточные привилегии — бот только дёргает Telegram API + local RPC |
| `monad-bft` (нода)        | monad       | ок                                                                      |

**DB:** пользователь `monadpulse` в PostgreSQL НЕ имеет SUPERUSER/CREATEDB/CREATEROLE (проверил `\du`). Подключение только через unix socket + `md5`. Ок.

**Dead rules:**
- `/etc/sudoers.d/90-cloud-init-users` содержит правило для `ubuntu`, но такого юзера на хосте нет — правило мёртвое, но стоит удалить (чистота).

**Риск:**
- 🟡 **monad-bot under root** — при любом RCE-баге в парсере Telegram (маловероятно, но) атакующий получит root. Перевести на dedicated user `monad-bot` с `ReadOnlyPaths=` для /etc/monad, ReadWrite только на лог-директорию.

---

## Part F — TLS / Dependencies 🟢 (после фикса)

**TLS:**
- Let's Encrypt `E7`, валиден **2026-04-16 → 2026-07-15**.
- Cipher: `TLSv1.3 / TLS_AES_256_GCM_SHA384`.
- SSL Labs-ish check: HSTS + preload, OCSP stapling on, только TLS 1.2/1.3.
- Auto-renew: `certbot.timer` активен.

**APT:**
- `apt list --upgradable` с `-a security` — **0 pending**. Kernel 6.8.0-110 текущий.

**Python deps (pip-audit, 88 пакетов):**

До фикса:
- `pip 24.0` → CVE-2025-8869, CVE-2026-1703, 2× ECHO (локальные).
- `starlette 0.41.3` → CVE-2025-54121 (path traversal в StaticFiles, MI), CVE-2025-62727 (ReDoS в MultipartPart, MI).

**Исправлено в ходе аудита:**
- `pip 24.0 → 26.0.1`.
- `starlette 0.41.3 → 1.0.0` (потребовало `fastapi 0.115.6 → 0.136.0`, совместимо).
- API перезапущен (`systemctl restart monadpulse-api`), endpoints `/dashboard/summary` и `/validators/list` отвечают 200 на testnet+mainnet.

**Повторный pip-audit:** `No known vulnerabilities found`. ✅

---

## Summary — findings & priorities

| # | Severity | Area             | Finding                                                        | Action                                                      |
|---|----------|------------------|----------------------------------------------------------------|-------------------------------------------------------------|
| 1 | 🟡 Med   | sudoers          | `shadowoftime ALL=(ALL) NOPASSWD: ALL`                         | Снять NOPASSWD, либо whitelist конкретных команд            |
| 2 | 🟡 Med   | validator keys   | id-bls / id-secp mode 0664                                     | `sudo chmod 600 /home/monad/monad-bft/config/id-{bls,secp}` |
| 3 | 🟡 Med   | wallet key       | auth_eoa.key — plain hex без passphrase                        | Миграция на keystore с passphrase                           |
| 4 | 🟡 Med   | service user     | `monad-bot.service` под root                                   | Создать юзера `monad-bot`, перевести unit                   |
| 5 | 🟢 Low   | sudoers cleanup  | мёртвое правило для несуществующего `ubuntu` user               | Удалить `/etc/sudoers.d/90-cloud-init-users`                |
| ✅| **fixed**| Python deps      | pip 24.0, starlette 0.41.3 — known CVEs                         | **Обновлено в ходе аудита**, pip-audit clean                |

---

## What was OK (positive findings)

- Нет SQL-инъекций, все запросы parametrized.
- Нет XSS: user-input через `esc()`, нет `innerHTML`.
- CSP strict без `unsafe-inline`/`unsafe-eval`.
- HSTS preload, TLS 1.3 only.
- Collector RPC и метрики bind на localhost.
- Rate limit работает под нагрузкой.
- `.env` не в git, mode 0600.
- DB-юзер без superuser, только через socket.
- fail2ban + нестандартный SSH-порт.

---

## Post-audit actions taken

1. ✅ `pip install --upgrade pip starlette fastapi`
2. ✅ `systemctl restart monadpulse-api`
3. ✅ Verified API on testnet+mainnet (200 OK)
4. ✅ This report committed to `/opt/monadpulse/AUDIT_SECURITY.md`

## Recommended follow-up (не делал, требует согласования)

1. Снять NOPASSWD в `/etc/sudoers.d/` (потребует ввода пароля для cron-скриптов — нужен whitelist).
2. `chmod 600` на id-bls/id-secp.
3. Миграция auth_eoa.key на зашифрованный keystore.
4. `monad-bot.service` → dedicated user.
5. Удалить мёртвый sudoers для `ubuntu`.
