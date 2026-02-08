import { extractTsMs } from "../time.js";

describe("extractTsMs", () => {
  it("converts seconds to ms", () => {
    expect(extractTsMs({ timestamp: 1700000000 })).toBe(1700000000 * 1000);
  });

  it("keeps ms as ms", () => {
    expect(extractTsMs({ timestamp: 1700000000000 })).toBe(1700000000000);
  });

  it("parses numeric string", () => {
    expect(extractTsMs({ created_at: "1700000000" })).toBe(1700000000 * 1000);
  });

  it("parses ISO string", () => {
    const iso = "2024-01-15T10:30:00.000Z";
    expect(extractTsMs({ occurredAt: iso })).toBe(new Date(iso).getTime());
  });

  it("returns null when no usable timestamp", () => {
    expect(extractTsMs({})).toBeNull();
  });
});
