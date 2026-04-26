#!/usr/bin/env bash
# Recompute content hashes for static JS/CSS, rewrite HTML <script>/<link>
# refs to include ?v=<hash8>. Run after editing /var/www/monadpulse/js/*.js
# or /var/www/monadpulse/css/*.css — idempotent (running twice is a no-op
# unless content changed).
#
# Why: browsers cache by full URL including query string. New ?v= → new
# cache entry → user gets fresh code. Old ?v= still works if the file
# itself hasn't changed. Combined with nginx Cache-Control "public,
# immutable" on .js/.css, repeat visitors skip the network entirely
# while still getting a deploy's worth of changes within 0 seconds.
set -euo pipefail

WEB=/var/www/monadpulse

js_hash() { sha256sum "$1" | cut -c1-8; }

JS_HASH=$(js_hash "$WEB/js/app.js")
CSS_HASH=$(js_hash "$WEB/css/style.css")

cd "$WEB"
patched=0
for f in *.html; do
  if grep -qE '/(js/app\.js|css/style\.css)(\?v=[a-f0-9]+)?' "$f"; then
    sed -i -E \
      -e "s|/js/app\.js(\?v=[a-f0-9]+)?|/js/app.js?v=$JS_HASH|g" \
      -e "s|/css/style\.css(\?v=[a-f0-9]+)?|/css/style.css?v=$CSS_HASH|g" \
      "$f"
    patched=$((patched + 1))
  fi
done

echo "deploy_assets.sh — refs updated in $patched HTML files"
echo "  js/app.js     ?v=$JS_HASH  ($(wc -c < js/app.js) bytes)"
echo "  css/style.css ?v=$CSS_HASH  ($(wc -c < css/style.css) bytes)"
