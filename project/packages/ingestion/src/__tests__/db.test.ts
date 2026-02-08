import { insertEvents } from "../db.js";

function makeDbMock() {
  return {
    query: jest.fn(),
  } as any;
}

describe("insertEvents", () => {
  it("chunks inserts and returns inserted count", async () => {
    const db = makeDbMock();
    // simulate: first chunk inserts 2, second chunk inserts 1
    db.query
      .mockResolvedValueOnce({ rowCount: 2 })
      .mockResolvedValueOnce({ rowCount: 1 });

    const events = [
      { id: "a", raw: { a: 1 } },
      { id: "b", raw: { b: 1 } },
      { id: "c", raw: { c: 1 } },
    ];

    const inserted = await insertEvents(db, events, 2);

    expect(inserted).toBe(3);
    expect(db.query).toHaveBeenCalledTimes(2);
  });

  it("returns 0 if no events", async () => {
    const db = makeDbMock();
    const inserted = await insertEvents(db, [], 500);
    expect(inserted).toBe(0);
    expect(db.query).not.toHaveBeenCalled();
  });
});
