/**
 * An AsyncGenerator that can also be awaited directly.
 * - `for await (const p of gen)` iterates items one at a time
 * - `await gen` collects all items into an array
 */
export type CollectableAsyncGenerator<T> = AsyncGenerator<T, void, unknown> & PromiseLike<T[]>;

/** Wrap an async generator to make it awaitable (collects all items on await). */
export function collectable<T>(gen: AsyncGenerator<T, void, unknown>): CollectableAsyncGenerator<T> {
  return {
    [Symbol.asyncIterator]() { return gen; },
    [Symbol.asyncDispose]: async () => { await gen.return(undefined as void); },
    next: () => gen.next(),
    return: (v?: void) => gen.return(v as void),
    throw: (e: unknown) => gen.throw(e),
    then<TResult1 = T[], TResult2 = never>(
      resolve?: ((value: T[]) => TResult1 | PromiseLike<TResult1>) | null,
      reject?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
    ): Promise<TResult1 | TResult2> {
      return (async () => {
        const items: T[] = [];
        for await (const item of gen) items.push(item);
        return items;
      })().then(resolve, reject);
    },
  } as CollectableAsyncGenerator<T>;
}
