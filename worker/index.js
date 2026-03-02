/**
 * Cloudflare Worker — CORS proxy for Metabase API.
 * Forwards POST requests to metabase.wiom.in/api/dataset
 * and adds CORS headers so the GitHub Pages dashboard can call it.
 */

const METABASE_URL = 'https://metabase.wiom.in';
const ALLOWED_ORIGIN = 'https://shivanksood-prog.github.io';

export default {
  async fetch(request) {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(request),
      });
    }

    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405, headers: corsHeaders(request) });
    }

    const url = new URL(request.url);
    const target = METABASE_URL + url.pathname + url.search;

    // Forward the request to Metabase
    const body = await request.text();
    const resp = await fetch(target, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-KEY': request.headers.get('X-API-KEY') || '',
      },
      body,
    });

    // Return response with CORS headers
    const responseBody = await resp.text();
    return new Response(responseBody, {
      status: resp.status,
      headers: {
        ...Object.fromEntries(resp.headers),
        ...corsHeaders(request),
        'Content-Type': 'application/json',
      },
    });
  },
};

function corsHeaders(request) {
  const origin = request.headers.get('Origin') || '';
  return {
    'Access-Control-Allow-Origin': origin.startsWith(ALLOWED_ORIGIN) ? origin : ALLOWED_ORIGIN,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-KEY',
    'Access-Control-Max-Age': '86400',
  };
}
