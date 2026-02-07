function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export async function fetchWithRetry(
  url: string,
  opts: RequestInit,
  maxRetries = 6,
) {
  let attempt = 0;

  while (true) {
    attempt++;
    const res = await fetch(url, opts);

    if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
      if (attempt > maxRetries) return res;

      const retryAfter = res.headers.get("retry-after");
      const serverWaitMs = retryAfter
        ? Math.ceil(Number(retryAfter) * 1000)
        : 0;
      const backoffMs = Math.min(30_000, 250 * 2 ** (attempt - 1));
      const waitMs = Math.max(serverWaitMs, backoffMs);

      console.warn(
        `Retrying ${res.status} in ${waitMs}ms (attempt ${attempt})`,
      );
      await sleep(waitMs);
      continue;
    }

    return res;
  }
}
