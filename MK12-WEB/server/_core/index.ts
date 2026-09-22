import "dotenv/config";
import { spawn } from "node:child_process";
import express from "express";
import { createServer } from "http";
import * as http from "node:http";
import net from "net";
import { createExpressMiddleware } from "@trpc/server/adapters/express";
import { registerOAuthRoutes } from "./oauth";
import { registerStorageProxy } from "./storageProxy";
import { appRouter } from "../routers";
import { createContext } from "./context";
import { serveStatic, setupVite } from "./vite";

const RAVEN_PORT = parseInt(process.env.RAVEN_PYTHON_PORT || "5173", 10);

function startRavenServer() {
  const child = spawn("python3", ["server.py"], {
    cwd: process.cwd(),
    env: { ...process.env, PORT: String(RAVEN_PORT) },
    stdio: ["ignore", "ignore", "inherit"],
  });

  child.on("error", error => {
    console.error("[RAVEN] Python service failed to start:", error);
  });

  const shutdown = () => {
    if (!child.killed) child.kill("SIGTERM");
  };
  process.once("exit", shutdown);
  process.once("SIGINT", () => {
    shutdown();
    process.exit(0);
  });
  process.once("SIGTERM", () => {
    shutdown();
    process.exit(0);
  });

  return child;
}

function isRavenRoute(url: string) {
  const path = url.split("?", 1)[0];
  return (
    path === "/api" ||
    path.startsWith("/api/") ||
    path === "/cyclocane-proxy" ||
    path.startsWith("/cyclocane-proxy/") ||
    path.startsWith("/javascripts/") ||
    path.startsWith("/stylesheets/")
  );
}

function proxyToRaven(req: express.Request, res: express.Response) {
  const upstream = http.request(
    {
      hostname: "127.0.0.1",
      port: RAVEN_PORT,
      path: req.originalUrl || req.url,
      method: req.method,
      headers: { ...req.headers, host: `127.0.0.1:${RAVEN_PORT}` },
    },
    response => {
      res.status(response.statusCode || 502);
      Object.entries(response.headers).forEach(([name, value]) => {
        if (value !== undefined) res.setHeader(name, value);
      });
      response.pipe(res);
    },
  );

  upstream.on("error", error => {
    if (!res.headersSent) {
      res.status(503).json({ status: "unavailable", error: "RAVEN service is starting", detail: String(error) });
    } else {
      res.end();
    }
  });
  req.pipe(upstream);
}

function isPortAvailable(port: number): Promise<boolean> {
  return new Promise(resolve => {
    const server = net.createServer();
    server.listen(port, () => {
      server.close(() => resolve(true));
    });
    server.on("error", () => resolve(false));
  });
}

async function findAvailablePort(startPort: number = 3000): Promise<number> {
  for (let port = startPort; port < startPort + 20; port++) {
    if (await isPortAvailable(port)) {
      return port;
    }
  }
  throw new Error(`No available port found starting from ${startPort}`);
}

async function startServer() {
  const app = express();
  const server = createServer(app);
  startRavenServer();
  app.use((req, res, next) => {
    if (isRavenRoute(req.originalUrl || req.url)) return proxyToRaven(req, res);
    return next();
  });
  // Configure body parser with larger size limit for file uploads
  app.use(express.json({ limit: "50mb" }));
  app.use(express.urlencoded({ limit: "50mb", extended: true }));
  registerStorageProxy(app);
  registerOAuthRoutes(app);
  // tRPC API
  app.use(
    "/api/trpc",
    createExpressMiddleware({
      router: appRouter,
      createContext,
    })
  );
  // development mode uses Vite, production mode uses static files
  if (process.env.NODE_ENV === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  const preferredPort = parseInt(process.env.PORT || "3000");
  const port = await findAvailablePort(preferredPort);

  if (port !== preferredPort) {
    console.log(`Port ${preferredPort} is busy, using port ${port} instead`);
  }

  server.listen(port, () => {
    console.log(`Server running on http://localhost:${port}/`);
  });
}

startServer().catch(console.error);
