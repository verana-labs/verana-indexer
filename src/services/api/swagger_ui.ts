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

      "GET openapi.json": async function (_req: IncomingMessage, res: ServerResponse) {
        const localPath = path.join(process.cwd(), openApiRelativePath);

        try {
          if (fs.existsSync(localPath)) {
            const data = await fs.promises.readFile(localPath, "utf8");
            res.setHeader("Content-Type", "application/json; charset=utf-8");
            res.end(data);
            return;
          }
        } catch {

        }


        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const svc: any = this as any;
        const spec = svc?.settings?.openapi ?? svc?.schema?.openapi ?? null;

        if (!spec) {
          res.statusCode = 404;
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ error: "OpenAPI spec not available", code: 404 }));
          return;
        }

        res.setHeader("Content-Type", "application/json; charset=utf-8");
        res.end(JSON.stringify(spec));
      },
    },

    mappingPolicy: "restrict" as const,
  };
}
