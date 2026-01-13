/**
 * Generates TypeScript types for ClickHouse settings by parsing Settings.cpp
 * from the official ClickHouse repository.
 *
 * Usage:
 *   node --experimental-strip-types scripts/generate-settings-types.ts
 *   node --experimental-strip-types scripts/generate-settings-types.ts --no-cache  # Force re-fetch all versions
 */

import { writeFileSync, readFileSync, existsSync } from "node:fs";

const BASELINE = { year: 24, month: 11 };

const SETTINGS_CPP_URL = (tag: string) =>
  `https://raw.githubusercontent.com/ClickHouse/ClickHouse/${tag}/src/Core/Settings.cpp`;
const SETTINGS_ENUMS_CPP_URL = (tag: string) =>
  `https://raw.githubusercontent.com/ClickHouse/ClickHouse/${tag}/src/Core/SettingsEnums.cpp`;

function versionToTag(version: string): string {
  return `v${version}.1.1-new`;
}

const VERSION_CACHE_FILE = "settings-versions.json";
const OUTPUT_FILE = "settings.generated.ts";

interface Setting {
  name: string;
  type: string;
  description: string;
  tsType: string;
  since?: string;
}

interface VersionCache {
  baseline: string;
  lastChecked: string;
  settings: Record<string, string>; // name -> first version seen
}

// Type mapping from C++ to TypeScript
const TYPE_MAP: Record<string, string> = {
  Bool: "boolean",
  UInt64: "bigint",
  Int64: "bigint",
  NonZeroUInt64: "bigint",
  Float: "number",
  Double: "number",
  String: "string",
  Seconds: "number",
  Milliseconds: "number",
  MaxThreads: "number",
  Map: "string", // Serialized as JSON-like string: '{"key": "value"}'
};

function* generateVersions(startYear: number, startMonth: number): Generator<string> {
  const now = new Date();
  const currentYear = now.getFullYear() % 100;
  const currentMonth = now.getMonth() + 1;

  for (let y = startYear; y <= currentYear; y++) {
    const start = y === startYear ? startMonth : 1;
    const end = y === currentYear ? currentMonth : 12;

    for (let m = start; m <= end; m++) {
      yield `${y}.${m}`;
    }
  }
}

async function fetchWithRetry(url: string, retries = 2): Promise<string | null> {
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await fetch(url);
      if (res.status === 404) return null;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (e) {
      if (i === retries) throw e;
      await new Promise((r) => setTimeout(r, 1000 * (i + 1)));
    }
  }
  return null;
}

function parseEnums(enumsCpp: string): Map<string, string[]> {
  const enums = new Map<string, string[]>();

  // Match IMPLEMENT_SETTING_ENUM(EnumName, ..., {{"value1", ...}, {"value2", ...}})
  // Pattern captures the enum name and the entire block of value mappings
  const implPattern = /IMPLEMENT_SETTING_(?:MULTI_)?ENUM\s*\(\s*(\w+)\s*,\s*\w+::\w+\s*,\s*\{([\s\S]*?)\}\s*\)/g;

  for (const match of enumsCpp.matchAll(implPattern)) {
    const enumName = match[1];
    const valuesBlock = match[2];

    // Extract string values: {"value_name", EnumType::...}
    const valuePattern = /\{\s*"([^"]*)"\s*,/g;
    const values = [...valuesBlock.matchAll(valuePattern)]
      .map((m) => m[1])
      .filter((v) => v.length > 0); // Filter out empty strings

    if (values.length > 0) {
      enums.set(enumName, values);
    }
  }

  return enums;
}

function parseSettings(cpp: string, enums: Map<string, string[]>): Setting[] {
  const settings: Setting[] = [];

  // Match DECLARE and DECLARE_WITH_ALIAS macros
  // DECLARE(Type, name, default, R"(description)", flags)
  const declarePattern =
    /DECLARE(?:_WITH_ALIAS)?\s*\(\s*(\w+)\s*,\s*(\w+)\s*,\s*[^,]+\s*,\s*R"\(\s*([\s\S]*?)\s*\)"\s*,\s*[^)]+\)/g;

  for (const match of cpp.matchAll(declarePattern)) {
    const [, cppType, name, description] = match;

    // Map C++ type to TypeScript
    let tsType = TYPE_MAP[cppType];
    if (!tsType) {
      // Check if it's an enum type
      const enumValues = enums.get(cppType);
      if (enumValues && enumValues.length > 0) {
        tsType = enumValues.map((v) => `'${v}'`).join(" | ");
      } else {
        // Unknown type, fall back to string
        tsType = "string";
      }
    }

    settings.push({
      name,
      type: cppType,
      description: cleanDescription(description),
      tsType,
    });
  }

  return settings;
}

function cleanDescription(desc: string): string {
  return desc
    .replace(/\n/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\\/g, "")
    .trim();
}

function loadVersionCache(): VersionCache | null {
  if (!existsSync(VERSION_CACHE_FILE)) return null;
  try {
    return JSON.parse(readFileSync(VERSION_CACHE_FILE, "utf-8"));
  } catch {
    return null;
  }
}

function saveVersionCache(cache: VersionCache): void {
  writeFileSync(VERSION_CACHE_FILE, JSON.stringify(cache, null, 2) + "\n");
}

async function buildVersionMap(noCache: boolean): Promise<Map<string, string>> {
  const cache = noCache ? null : loadVersionCache();
  const firstSeen = new Map<string, string>(
    cache ? Object.entries(cache.settings) : []
  );

  // Determine starting point
  let startYear = BASELINE.year;
  let startMonth = BASELINE.month;

  if (cache && !noCache) {
    const [y, m] = cache.lastChecked.split(".").map(Number);
    if (m === 12) {
      startYear = y + 1;
      startMonth = 1;
    } else {
      startYear = y;
      startMonth = m + 1;
    }
    console.log(`Resuming from v${startYear}.${startMonth} (cached up to v${cache.lastChecked})`);
  } else {
    console.log(`Building version map from v${startYear}.${startMonth}...`);
  }

  let lastSuccessfulVersion = cache?.lastChecked || `${startYear}.${startMonth}`;
  let versionsChecked = 0;

  for (const version of generateVersions(startYear, startMonth)) {
    const tag = versionToTag(version);
    process.stdout.write(`  Checking ${tag}...`);

    const cpp = await fetchWithRetry(SETTINGS_CPP_URL(tag));
    if (!cpp) {
      console.log(" not found");
      continue;
    }

    // Quick parse to get setting names
    const names = [...cpp.matchAll(/DECLARE(?:_WITH_ALIAS)?\s*\(\s*\w+\s*,\s*(\w+)/g)].map(
      (m) => m[1]
    );

    let newSettings = 0;
    for (const name of names) {
      if (!firstSeen.has(name)) {
        firstSeen.set(name, version);
        newSettings++;
      }
    }

    console.log(` ${names.length} settings (${newSettings} new)`);
    lastSuccessfulVersion = version;
    versionsChecked++;
  }

  // Save cache
  const newCache: VersionCache = {
    baseline: `${BASELINE.year}.${BASELINE.month}`,
    lastChecked: lastSuccessfulVersion,
    settings: Object.fromEntries(firstSeen),
  };
  saveVersionCache(newCache);
  console.log(`Cached ${firstSeen.size} settings (checked ${versionsChecked} versions)`);

  return firstSeen;
}

function generateTypeScript(
  settings: Setting[],
  versionMap: Map<string, string>,
  latestTag: string
): string {
  const baseline = `${BASELINE.year}.${BASELINE.month}`;

  const lines: string[] = [
    `// AUTO-GENERATED from ClickHouse ${latestTag} Settings.cpp`,
    `// Run: make update-settings`,
    `// Do not edit manually.`,
    ``,
    `/**`,
    ` * Typed ClickHouse settings interface.`,
    ` * Generated from official ClickHouse source code.`,
    ` */`,
    `export interface ClickHouseSettings {`,
  ];

  // Sort settings alphabetically
  const sorted = [...settings].sort((a, b) => a.name.localeCompare(b.name));

  for (const setting of sorted) {
    const since = versionMap.get(setting.name);
    const sinceTag = since && since !== baseline ? `\n   * @since ${since}` : "";

    lines.push(`  /**`);
    lines.push(`   * ${setting.description}${sinceTag}`);
    lines.push(`   */`);
    lines.push(`  ${setting.name}?: ${setting.tsType};`);
    lines.push(``);
  }

  // Add index signature to allow unknown settings (preserves autocomplete for known ones)
  lines.push(`  /** Index signature for unknown/custom settings */`);
  lines.push(`  [key: string]: unknown;`);
  lines.push(`}`);
  lines.push(``);

  return lines.join("\n");
}

async function main() {
  const noCache = process.argv.includes("--no-cache");

  console.log("Building version map...");
  const versionMap = await buildVersionMap(noCache);

  // Get latest version for full parsing
  const versions = [...generateVersions(BASELINE.year, BASELINE.month)];
  let latestVersion = versions[versions.length - 1];
  let latestTag = versionToTag(latestVersion);
  let cpp: string | null = null;
  let enumsCpp: string | null = null;

  // Try latest versions until we find one that exists
  for (let i = versions.length - 1; i >= 0 && !cpp; i--) {
    latestVersion = versions[i];
    latestTag = versionToTag(latestVersion);
    cpp = await fetchWithRetry(SETTINGS_CPP_URL(latestTag));
    if (cpp) {
      enumsCpp = await fetchWithRetry(SETTINGS_ENUMS_CPP_URL(latestTag));
    }
  }

  if (!cpp) {
    throw new Error("Could not fetch Settings.cpp from any version");
  }

  console.log(`\nParsing ${latestTag} for full type information...`);

  const enums = enumsCpp ? parseEnums(enumsCpp) : new Map();
  console.log(`  Found ${enums.size} enum types`);

  const settings = parseSettings(cpp, enums);
  console.log(`  Found ${settings.length} settings`);

  const ts = generateTypeScript(settings, versionMap, latestTag);
  writeFileSync(OUTPUT_FILE, ts);
  console.log(`\nWrote ${OUTPUT_FILE}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
