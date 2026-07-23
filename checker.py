import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
import time
import os
import re

TIMEOUT = 12
CONCURRENCY_LIMIT = 50
TEST_URL = "https://www.gstatic.com/generate_204"

async def fetch_url_content(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                return await response.text()
    except:
        return ""

def clean_proxy_list(raw_data, default_proto):
    proxies = []
    lines = re.split(r'[\n,\s\r]+', raw_data.strip())
    for line in lines:
        line = line.strip()
        if not line: continue
        if not re.match(r'^[a-zA-Z0-9]+://', line):
            formatted_proxy = f"{default_proto}://{line}"
        else:
            formatted_proxy = line
        proxies.append(formatted_proxy)
    return list(set(proxies))

async def check_proxy(proxy_url, sem):
    async with sem:
        results = {"proxy": proxy_url, "tcp": False, "http": False, "latency": 99999}
        start_time = time.time()
        try:
            # پاکسازی آدرس برای تست TCP
            clean_url = proxy_url.split("://")[-1].split('/')[0]
            if ":" not in clean_url: return results
            host, port = clean_url.split(":")
            
            # TCP Test
            try:
                conn = asyncio.open_connection(host, int(port))
                reader, writer = await asyncio.wait_for(conn, timeout=5)
                writer.close()
                await writer.wait_closed()
                results["tcp"] = True
            except:
                return results

            # HTTP Test
            connector = ProxyConnector.from_url(proxy_url) if "socks" in proxy_url else None
            async with aiohttp.ClientSession(connector=connector) as session:
                proxy_arg = None if connector else proxy_url
                async with session.get(TEST_URL, proxy=proxy_arg, timeout=TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'}) as response:
                    if response.status in [200, 204]:
                        results["http"] = True
                        results["latency"] = int((time.time() - start_time) * 1000)
        except:
            pass
        return results

async def main():
    raw_input = os.getenv("PROXY_INPUT", "")
    default_proto = os.getenv("DEFAULT_PROTO", "http")
    
    if not raw_input:
        print("Error: PROXY_INPUT is empty")
        return

    if raw_input.startswith("http"):
        content = await fetch_url_content(raw_input)
        if content: raw_input = content
        
    proxies_to_test = clean_proxy_list(raw_input, default_proto)
    print(f"Testing {len(proxies_to_test)} proxies...")

    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = [check_proxy(p, sem) for p in proxies_to_test]
    results = await asyncio.gather(*tasks)
    
    all_pass = sorted([r for r in results if r['tcp'] and r['http']], key=lambda x: x['latency'])
    tcp_only = [r for r in results if r['tcp']]
    http_only = [r for r in results if r['http']]
    
    os.makedirs("results", exist_ok=True)
    with open("results/all.txt", "w") as f: f.write("\n".join([r['proxy'] for r in all_pass]))
    with open("results/tcp_pass.txt", "w") as f: f.write("\n".join([r['proxy'] for r in tcp_only]))
    with open("results/http_pass.txt", "w") as f: f.write("\n".join([r['proxy'] for r in http_only]))
    print(f"Success! {len(all_pass)} proxies saved.")

if __name__ == "__main__":
    asyncio.run(main())
