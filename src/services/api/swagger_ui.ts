import fs from "fs";
import { IncomingMessage, ServerResponse } from "http";
import path from "path";
// eslint-disable-next-line import/no-extraneous-dependencies
import { getAbsoluteFSPath as swaggerDistPath } from "swagger-ui-dist";

export function swaggerUiComponent(openApiRelativePath = "docs/api/openapi.json") {
  const swaggerPath = swaggerDistPath();

  return {
    aliases: {
      "GET /": (_req: IncomingMessage, res: ServerResponse) => {
        const html = `<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Verana Indexer API Docs</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
    <link rel="stylesheet" href="/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="/swagger-ui-bundle.js"></script>
    <script src="/swagger-ui-standalone-preset.js"></script>
    <script>
      window.onload = function() {
        SwaggerUIBundle({
          url: '/openapi.json',
          dom_id: '#swagger-ui',
          presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
          layout: 'BaseLayout'
        });
      };
    </script>
  </body>
</html>`;
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        res.end(html);
      },
      "GET /favicon.svg": (req: IncomingMessage, res: ServerResponse) => {
        const localPath = path.join(process.cwd(), "docs", "favicon.svg");
        if (!fs.existsSync(localPath)) {
          res.statusCode = 404;
          res.end("Not Found");
          return;
        }
        res.setHeader("Content-Type", "image/svg+xml; charset=utf-8");
        fs.createReadStream(localPath).pipe(res);
      },

      "GET :file": (req: IncomingMessage, res: ServerResponse) => {
        const file = (req.url || "").replace(/^\//, "");
        const filePath = path.join(swaggerPath, file);

        if (!fs.existsSync(filePath)) {
          res.statusCode = 404;
          res.end("Not Found");
          return;
        }

        const ext = path.extname(file);
        const type =
          ext === ".css"
            ? "text/css"
            : ext === ".js"
              ? "application/javascript"
              : "text/plain";

        res.setHeader("Content-Type", `${type}; charset=utf-8`);
        fs.createReadStream(filePath).pipe(res);
      },

      "GET openapi.json": async function (req: IncomingMessage, res: ServerResponse) {
        const localPath = path.join(process.cwd(), openApiRelativePath);

        const getServerUrl = (): string => {
          if (process.env.API_URL) {
            return process.env.API_URL;
          }
          if (process.env.SERVER_URL) {
            return process.env.SERVER_URL;
          }
          
          const host = req.headers.host || `localhost:${process.env.PORT || 3001}`;
          
          const isSecure = req.headers['x-forwarded-proto'] === 'https' || 
                          req.headers['x-forwarded-ssl'] === 'on' ||
                          (req.connection as any)?.encrypted === true;
          const protocol = isSecure ? 'https' : 'http';
          
          return `${protocol}://${host}`;
        };

        const getServers = (): Array<{ url: string; description: string }> => {
          const currentUrl = getServerUrl();
          const servers: Array<{ url: string; description: string }> = [];
          const addedUrls = new Set<string>();
          
          const env = process.env.NODE_ENV || 'development';
          let envDescription = 'Local development server';
          
          if (env === 'production') {
            envDescription = 'Production API server';
          } else if (env === 'test' || currentUrl.includes('testnet')) {
            envDescription = 'Testnet API server';
          } else if (currentUrl.includes('devnet')) {
            envDescription = 'Devnet API server';
          }
          
          servers.push({
            url: currentUrl,
            description: envDescription
          });
          addedUrls.add(currentUrl);
          
          if (process.env.DEVNET_API_URL && !addedUrls.has(process.env.DEVNET_API_URL)) {
            servers.push({
              url: process.env.DEVNET_API_URL,
              description: 'Devnet API server'
            });
            addedUrls.add(process.env.DEVNET_API_URL);
          }
          
          if (process.env.TESTNET_API_URL && !addedUrls.has(process.env.TESTNET_API_URL)) {
            servers.push({
              url: process.env.TESTNET_API_URL,
              description: 'Testnet API server'
            });
            addedUrls.add(process.env.TESTNET_API_URL);
          }
          
          if (env !== 'production') {
            const defaultDevnet = 'https://idx.devnet.verana.network';
            const defaultTestnet = 'https://idx.testnet.verana.network';
            
            if (!addedUrls.has(defaultDevnet) && 
                !currentUrl.includes('devnet.verana.network')) {
              servers.push({
                url: defaultDevnet,
                description: 'Devnet API server'
              });
              addedUrls.add(defaultDevnet);
            }
            
            if (!addedUrls.has(defaultTestnet) && 
                !currentUrl.includes('testnet.verana.network')) {
              servers.push({
                url: defaultTestnet,
                description: 'Testnet API server'
              });
              addedUrls.add(defaultTestnet);
            }
          }
          
          return servers;
        };

        try {
          let spec: any = null;
          
          if (fs.existsSync(localPath)) {
            const data = await fs.promises.readFile(localPath, "utf8");
            spec = JSON.parse(data);
          } else {
            // Fallback to service-generated spec
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const svc: any = this as any;
            spec = svc?.settings?.openapi ?? svc?.schema?.openapi ?? null;
          }

          if (!spec) {
            res.statusCode = 404;
            res.setHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ error: "OpenAPI spec not available", code: 404 }));
            return;
          }

          spec.servers = getServers();

          res.setHeader("Content-Type", "application/json; charset=utf-8");
          res.end(JSON.stringify(spec));
        } catch (err: any) {
          res.statusCode = 500;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ error: `Failed to load OpenAPI spec: ${err.message}`, code: 500 }));
        }
      },
    },

    mappingPolicy: "restrict" as const,
  };
}
