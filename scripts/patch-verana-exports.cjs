const fs = require("fs");
const path = require("path");

const packageJsonPath = path.join(
  __dirname,
  "..",
  "node_modules",
  "@verana-labs",
  "verana-types",
  "package.json"
);

if (fs.existsSync(packageJsonPath)) {
  try {
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
    packageJson.exports = {
      ".": "./dist/index.js",
      "./codec/*": "./dist/codec/*.js"
    };
    
    fs.writeFileSync(
      packageJsonPath,
      JSON.stringify(packageJson, null, 2) + "\n",
      "utf8"
    );
    
    console.log("✅ Patched @verana-labs/verana-types package.json exports");
  } catch (err) {
    console.warn("⚠️  Failed to patch package.json:", err.message);
  }
} else {
  console.warn("⚠️  @verana-labs/verana-types package.json not found, skipping patch");
}

