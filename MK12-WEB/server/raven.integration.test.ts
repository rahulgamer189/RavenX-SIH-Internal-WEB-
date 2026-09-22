import { afterEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

let child: ChildProcess | undefined;

async function waitForHealth(url: string) {
  const deadline = Date.now() + 8_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) return response.json();
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }

  throw lastError ?? new Error("RAVEN health endpoint did not become ready");
}

afterEach(() => {
  child?.kill("SIGTERM");
  child = undefined;
});

describe("RAVEN Python service", () => {
  it("starts and reports a healthy service", async () => {
    const port = 5300 + Math.floor(Math.random() * 200);
    child = spawn("python3", ["server.py"], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        PORT: String(port),
        RAVEN_REFRESH_SECONDS: "86400",
      },
      stdio: "ignore",
    });

    await expect(waitForHealth(`http://127.0.0.1:${port}/api/healthz`)).resolves.toMatchObject({
      status: "ok",
      service: "RAVEN",
    });
  });
});
