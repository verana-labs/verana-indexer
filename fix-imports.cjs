const fs = require("fs");
const path = require("path");

// Recursively collect all .js files under a directory
function findJsFiles(dir, files = []) {
  const items = fs.readdirSync(dir);
  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      findJsFiles(fullPath, files);
    } else if (item.endsWith(".js")) {
      files.push(fullPath);
    }
  }
  return files;
}

// Check if path is a directory
function isDirectory(basePath, importPath) {
  try {
    const fullPath = path.resolve(basePath, importPath);
    return fs.statSync(fullPath).isDirectory();
  } catch {
    return false;
  }
}

// Determine if an import path needs a .js or /index.js extension
function needsJsExtension(importPath) {
  if (importPath === "@cosmjs/cosmwasm-stargate/build/modules") {
    return "index";
  }

  const cosmjsPaths = [
    "@cosmjs/tendermint-rpc/build/jsonrpc",
    "@cosmjs/tendermint-rpc/build/rpcclients",
    "@cosmjs/tendermint-rpc/build/tendermint34",
    "@cosmjs/tendermint-rpc/build/tendermint37",
    "@cosmjs/tendermint-rpc/build/types",
    "@cosmjs/tendermint-rpc/build/addresses",
    "@cosmjs/tendermint-rpc/build/dates",
    "@cosmjs/tendermint-rpc/build/tendermintclient",
  ];
  if (cosmjsPaths.includes(importPath)) return "file";

  if (
    importPath.startsWith("stream-json/") ||
    importPath.startsWith("stream-chain/")
  ) {
    return "file";
  }

  if (importPath === "protobufjs/minimal") {
    return "file";
  }
  return false;
}

// Fix imports/exports in a single file
function fixImportsInFile(filePath) {
  try {
    let content = fs.readFileSync(filePath, "utf8");
    let modified = false;
    const fileDir = path.dirname(filePath);

    // Match both import and export statements
    const importExportRegex =
      /\b(import|export)\s+(?:[\s\S]+?\s+from\s+)?['"]([^'"]+)['"]/g;

    content = content.replace(
      importExportRegex,
      (match, keyword, importPath) => {
        // Skip if already .js or .json
        if (importPath.endsWith(".js") || importPath.endsWith(".json")) {
          return match;
        }

        // Handle known packages
        const jsExtType = needsJsExtension(importPath);
        if (jsExtType) {
          modified = true;
          if (jsExtType === "index") {
            return match.replace(importPath, `${importPath}/index.js`);
          }
          return match.replace(importPath, `${importPath}.js`);
        }

        // Handle relative imports (./ or ../)
        if (importPath.startsWith("./") || importPath.startsWith("../")) {
          modified = true;
          if (isDirectory(fileDir, importPath)) {
            return match.replace(importPath, `${importPath}/index.js`);
          } else {
            return match.replace(importPath, `${importPath}.js`);
          }
        }

        return match; // leave other imports untouched
      }
    );

    if (modified) {
      fs.writeFileSync(filePath, content, "utf8");
      console.log(`Fixed imports in: ${filePath}`);
    }
  } catch (err) {
    console.error(`Error processing ${filePath}:`, err.message);
  }
}

// --- Main execution ---
console.log(
  "🔧 Fixing missing .js or /index.js extensions in compiled files..."
);
const distDir = path.join(__dirname, "dist");

if (!fs.existsSync(distDir)) {
  console.error("❌ dist directory not found! Please run your build first.");
}

const jsFiles = findJsFiles(distDir);
console.log(`📁 Found ${jsFiles.length} JavaScript files to process`);

jsFiles.forEach(fixImportsInFile);

console.log("✅ All files processed. Try running pnpm start again.");
