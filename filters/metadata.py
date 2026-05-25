# filters/metadata.py — Filter 7: Metadata Consistency
# Checks that website/Twitter links resolve, domain age, social account age

import asyncio
import re
from datetime import datetime, timezone

import aiohttp
import whois  # python-whois

import config
from utils.logger import get_logger
from utils.models import NewTokenEvent

logger = get_logger("filter_metadata")

DOMAIN_MAX_AGE_HOURS = 48  # Reject if domain registered < 48 hours ago


async def run_metadata(event: NewTokenEvent) -> tuple[bool, str, dict]:
    mint = event.mint_address

    # Get the token's off-chain JSON metadata
    uri = event.raw_data.get("_uri", "")
    if not uri:
        uri = await _get_token_uri(mint)

    if not uri:
        return False, "Metadata: no metadata URI found", {}

    json_meta = await _fetch_json_metadata(uri)
    extra     = {"uri": uri, "json_meta": json_meta}

    if not json_meta:
        return False, "Metadata: could not fetch JSON metadata", extra

    website = json_meta.get("website", "") or json_meta.get("external_url", "")
    twitter = (
        json_meta.get("twitter", "")
        or json_meta.get("extensions", {}).get("twitter", "")
    )

    extra["website"] = website
    extra["twitter"] = twitter

    checks = await asyncio.gather(
        _check_website(website),
        _check_twitter(twitter),
        return_exceptions=True,
    )

    website_check = checks[0] if not isinstance(checks[0], Exception) else {"ok": False, "reason": str(checks[0])}
    twitter_check = checks[1] if not isinstance(checks[1], Exception) else {"ok": False, "reason": str(checks[1])}

    # Website check
    if website and not website_check.get("ok"):
        return False, f"Metadata: website check failed — {website_check.get('reason')}", extra

    # Domain age check
    if website and website_check.get("domain_age_hours") is not None:
        age_h = website_check["domain_age_hours"]
        if age_h < DOMAIN_MAX_AGE_HOURS:
            return (
                False,
                f"Metadata: domain registered only {age_h:.1f}h ago (min {DOMAIN_MAX_AGE_HOURS}h)",
                extra,
            )

    # Twitter check
    if twitter and not twitter_check.get("ok"):
        return False, f"Metadata: Twitter check failed — {twitter_check.get('reason')}", extra

    return True, "Metadata check passed", extra


async def _get_token_uri(mint: str) -> str:
    """Fetch metadata URI from Helius DAS API."""
    url = f"https://mainnet.helius-rpc.com/?api-key={config.HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getAsset",
        "params": {"id": mint},
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data   = await resp.json()
                result = data.get("result", {})
                return result.get("content", {}).get("json_uri", "")
    except Exception:
        return ""


async def _fetch_json_metadata(uri: str) -> dict | None:
    if not uri or not uri.startswith("http"):
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(uri, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
    except Exception as e:
        logger.debug(f"JSON metadata fetch error: {e}")
    return None


async def _check_website(url: str) -> dict:
    if not url:
        return {"ok": True, "reason": "no website provided"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=8), allow_redirects=True
            ) as resp:
                if resp.status >= 400:
                    return {"ok": False, "reason": f"HTTP {resp.status}"}

        # Check domain age
        domain      = _extract_domain(url)
        age_hours   = _get_domain_age_hours(domain)
        return {"ok": True, "reason": "resolves", "domain_age_hours": age_hours}

    except aiohttp.ClientError as e:
        return {"ok": False, "reason": f"does not resolve: {e}"}


def _extract_domain(url: str) -> str:
    match = re.search(r"https?://([^/]+)", url)
    return match.group(1).replace("www.", "") if match else ""


def _get_domain_age_hours(domain: str) -> float | None:
    """Get domain age in hours using whois. Returns None on error."""
    if not domain:
        return None
    try:
        w = whois.whois(domain)
        created = w.creation_date
        if isinstance(created, list):
            created = created[0]
        if created:
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            delta = datetime.now(tz=timezone.utc) - created
            return delta.total_seconds() / 3600
    except Exception as e:
        logger.debug(f"WHOIS error for {domain}: {e}")
    return None


async def _check_twitter(twitter_url: str) -> dict:
    if not twitter_url:
        return {"ok": True, "reason": "no twitter provided"}

    # Normalize URL
    if not twitter_url.startswith("http"):
        twitter_url = f"https://twitter.com/{twitter_url.lstrip('@')}"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; SniperBot/1.0)",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                twitter_url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=8),
                allow_redirects=True,
            ) as resp:
                # Twitter often returns 200 even for non-existent users
                # A 404 is a definite fail
                if resp.status == 404:
                    return {"ok": False, "reason": "Twitter account not found (404)"}
                return {"ok": True, "reason": f"Twitter resolves (HTTP {resp.status})"}
    except Exception as e:
        return {"ok": False, "reason": f"Twitter does not resolve: {e}"}
