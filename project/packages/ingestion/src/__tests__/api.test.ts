import { fetchOnePage } from "../api.js";

// mock fetchWithRetry used by api.ts
jest.mock("../http.js", () => ({
  fetchWithRetry: jest.fn(),
}));

import { fetchWithRetry } from "../http.js";

function mockResponse(
  body: any,
  headers: Record<string, string> = {},
  ok = true,
  status = 200,
) {
  return {
    ok,
    status,
    text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
    headers: {
      get: (k: string) => headers[k.toLowerCase()] ?? headers[k] ?? null,
    },
  };
}

describe("fetchOnePage", () => {
  beforeEach(() => {
    (fetchWithRetry as jest.Mock).mockReset();
  });

  it("parses events from json.data array + nextCursor", async () => {
    (fetchWithRetry as jest.Mock).mockResolvedValueOnce(
      mockResponse(
        {
          data: [{ id: "a" }, { id: "b" }],
          pagination: { nextCursor: "c1", cursorExpiresIn: 100 },
          meta: { total: 10 },
        },
        {
          "x-ratelimit-limit": "10",
          "x-ratelimit-remaining": "9",
          "x-ratelimit-reset": "60",
        },
      ),
    );

    const res = await fetchOnePage({
      baseUrl: "http://x/api/v1",
      apiKey: "k",
      cursor: null,
      limit: 1000,
    });

    expect(res.events).toHaveLength(2);
    expect(res.nextCursor).toBe("c1");
    expect(res.cursorExpiresIn).toBe(100);
    expect(res.total).toBe(10);
    expect(res.rateLimit.remaining).toBe(9);
  });

  it("sanitizes NaN in response", async () => {
    (fetchWithRetry as jest.Mock).mockResolvedValueOnce(
      mockResponse(
        '{"data":[{"id":"a"}],"meta":{"total":3000000},"x":NaN,"pagination":{"nextCursor":"c"}}',
      ),
    );

    const res = await fetchOnePage({
      baseUrl: "http://x/api/v1",
      apiKey: "k",
      cursor: null,
      limit: 1000,
    });
    expect(res.events[0].id).toBe("a");
    expect(res.nextCursor).toBe("c");
  });

  it("throws BAD_JSON for truly broken JSON", async () => {
    (fetchWithRetry as jest.Mock).mockResolvedValueOnce(
      mockResponse("{not json"),
    );

    await expect(
      fetchOnePage({
        baseUrl: "http://x/api/v1",
        apiKey: "k",
        cursor: null,
        limit: 1000,
      }),
    ).rejects.toThrow(/BAD_JSON/);
  });
});
