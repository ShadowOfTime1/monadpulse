# Cloudflare front for monadpulse.xyz — setup steps

Server-side prep is already done (`/etc/nginx/conf.d/cloudflare.conf` —
trusted IP ranges + `real_ip_header CF-Connecting-IP`). The dashboard
+ DNS steps below are manual.

---

## 1. Add the site to Cloudflare (5 min)

1. https://dash.cloudflare.com → **Add a site** → enter `monadpulse.xyz`.
2. Pick the **Free plan** ($0). Everything we need is on free:
   - Global CDN (~280 PoPs, including 6 in Australia/NZ)
   - Universal SSL
   - Always Online
   - DDoS protection
   - Analytics
3. Cloudflare scans existing DNS records. Verify these come across:
   ```
   A     monadpulse.xyz       51.161.174.79     Proxied (orange cloud)
   A     www.monadpulse.xyz   51.161.174.79     Proxied (orange cloud)
   ```
   If the orange cloud is grey, click it — proxy must be ON for CDN to work.
4. Cloudflare gives you 2 nameservers, e.g.:
   ```
   olga.ns.cloudflare.com
   pablo.ns.cloudflare.com
   ```

## 2. Switch nameservers at OVH (5 min, propagation up to 24h)

1. https://www.ovh.com/manager/ → Web Cloud → Domains → `monadpulse.xyz` → **DNS servers** tab.
2. Click **Modify DNS servers** → switch to **Custom**.
3. Replace OVH nameservers with the two from Cloudflare.
4. Save. Propagation usually 5-30 min on OVH; CF will email when active.

## 3. Cloudflare SSL/TLS settings

In Cloudflare dashboard for monadpulse.xyz:

- **SSL/TLS → Overview**: set mode to **Full (strict)**.
  - Origin already has a valid Let's Encrypt cert
    (`/etc/letsencrypt/live/monadpulse.xyz/`), so end-to-end encryption
    with cert validation works.
- **SSL/TLS → Edge Certificates**: enable **Always Use HTTPS** and
  **Automatic HTTPS Rewrites**.
- **SSL/TLS → Edge Certificates → Min TLS**: 1.2

## 4. Caching settings

In **Caching → Configuration**:
- Caching Level: **Standard** (CF respects our `Cache-Control` headers)
- Browser Cache TTL: **Respect Existing Headers**
- Always Online: **On**

In **Speed → Optimization**:
- Auto Minify: **off** for HTML/JS/CSS — we already minified+gzipped server-side
- Brotli: **on** (cheap on CF edge)
- Early Hints: **on**

## 5. Page Rules (free plan gets 3)

1. `monadpulse.xyz/api/*` → **Cache Level: Bypass** (origin already
   handles per-endpoint caching — let it through unmodified).
2. `monadpulse.xyz/js/*` → **Edge Cache TTL: 1 month**
3. `monadpulse.xyz/css/*` → **Edge Cache TTL: 1 month**

## 6. Verify after switch

Once DNS propagates (`dig monadpulse.xyz +short` returns CF IPs like
104.21.x.x or 172.67.x.x):

```bash
# Headers should now show CF-Ray, cf-cache-status, server: cloudflare
curl -sI https://monadpulse.xyz/about.html | grep -iE 'server|cf-|cache'

# Static asset should hit edge cache on second request:
curl -s -D - https://monadpulse.xyz/js/app.js?v=aa22bbb0 -o /dev/null | grep -i cf-cache-status
# First: HIT or MISS, Second: HIT
```

In nginx logs (`/var/log/nginx/access.log`), the first column should now
show **real client IPs**, not CF edge IPs (108.162.x.x, 162.158.x.x etc.).
That confirms the CF-Connecting-IP header is being honoured.

## 7. Roll back (if needed)

Switch nameservers back to OVH defaults at the registrar — DNS reverts
in ~15 min. Server-side nginx config keeps working with or without CF
in front (the CF IP trust list is harmless when there's no CF).

---

## What this buys

| Audience | Before | After |
|---|---|---|
| Sydney / nearby | already fast (~30ms RTT) | identical |
| Europe | 280-320ms RTT to origin per request | ~30ms RTT to nearest CF PoP |
| Americas | 200-250ms RTT | ~20ms RTT |
| Asia | 80-150ms RTT | ~30ms RTT |

Static asset payload after gzip + immutable cache + CF edge ≈ 0 bytes
on repeat visit. First visit: ~28KB compressed, served from CF edge in
single intra-region round-trip.

API endpoints stay live (CF Page Rule bypasses /api/), so dashboard
ticker still polls origin every 2s. CF doesn't add latency to API calls,
it just enforces TLS and adds DDoS / abuse protection.
