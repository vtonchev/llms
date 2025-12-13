/**
 * Simple request throttle to rate limit API calls per provider
 * Ensures at least `minInterval` ms between requests to the same provider
 * Requests are still processed in parallel, just with a staggered start
 */

class RequestThrottle {
  private lastRequestTime: Map<string, number> = new Map();
  private minInterval: number;

  constructor(minIntervalMs: number = 100) {
    this.minInterval = minIntervalMs;
  }

  /**
   * Throttle a request - waits if needed to ensure minInterval between requests
   * Returns a promise that resolves when it's safe to make the request
   */
  async throttle(providerName: string): Promise<void> {
    const now = Date.now();
    const lastTime = this.lastRequestTime.get(providerName) || 0;
    const elapsed = now - lastTime;

    if (elapsed < this.minInterval) {
      const waitTime = this.minInterval - elapsed;
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }

    // Update last request time
    this.lastRequestTime.set(providerName, Date.now());
  }

  /**
   * Wrap an async function with throttling
   * Ensures minInterval between request starts, but allows parallel execution
   */
  async execute<T>(providerName: string, fn: () => Promise<T>): Promise<T> {
    await this.throttle(providerName);
    return fn();
  }
}

// Global throttle instance - ensures 100ms between requests per provider
export const requestThrottle = new RequestThrottle(100);

// Re-export for backwards compatibility with the queue name
export const requestQueue = {
  enqueue: <T>(providerName: string, fn: () => Promise<T>) =>
    requestThrottle.execute(providerName, fn),
  getQueueLength: () => 0, // No queue, just throttle
  isProcessing: () => false,
};
