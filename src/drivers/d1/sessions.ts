/**
 * D1 Sessions API Manager for Read-Your-Own-Writes (RYW) consistency.
 */
export class D1SessionManager {
  private currentBookmark: string | null = null;

  constructor(initialBookmark?: string | null) {
    if (initialBookmark) {
      this.currentBookmark = initialBookmark;
    }
  }

  /**
   * Sets or advances the session bookmark.
   */
  setBookmark(bookmark: string | null): void {
    if (bookmark) {
      this.currentBookmark = bookmark;
    }
  }

  /**
   * Retrieves the current bookmark.
   */
  getBookmark(): string | null {
    return this.currentBookmark;
  }

  /**
   * Clears the active bookmark.
   */
  clearBookmark(): void {
    this.currentBookmark = null;
  }

  /**
   * Wraps a D1Database instance with the current or explicitly provided session bookmark.
   */
  wrapDatabase(db: D1Database, bookmarkOverride?: string | null): D1Database {
    const bookmark = bookmarkOverride !== undefined ? bookmarkOverride : this.currentBookmark;
    if (bookmark && typeof (db as any).withSession === "function") {
      return (db as any).withSession(bookmark);
    }
    return db;
  }
}
