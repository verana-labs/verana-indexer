export default {
  branches: [
    "release",
    { name: "main" }, 
    { name: "dev", prerelease: "dev" },
  ],
  tagFormat: "v${version}", 
  plugins: [
    [
      "@semantic-release/commit-analyzer",
      {
        preset: "angular",
        releaseRules: [
          { type: "refactor", release: "patch" }
        ]
      }
    ],
    "@semantic-release/release-notes-generator",
    [
      "@semantic-release/npm",
      { npmPublish: false }
    ],
    [
      "@semantic-release/github",
      {
        assets: [
          { path: "dist/**/*", label: "Build files" }
        ]
      }
    ]
  ]
};
