#!/usr/bin/env node
/**
 * Parse V8 CPU profile and output hotspot functions.
 * Usage: node scripts/profile-hotspots.ts <profile.cpuprofile> [top=30]
 */

import { readFileSync } from "node:fs";

interface CallFrame {
  functionName: string;
  scriptId: string;
  url: string;
  lineNumber: number;
  columnNumber: number;
}

interface ProfileNode {
  id: number;
  callFrame: CallFrame;
  hitCount: number;
  children?: number[];
}

interface Profile {
  nodes: ProfileNode[];
  startTime: number;
  endTime: number;
  samples: number[];
  timeDeltas: number[];
}

function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) {
    console.error("Usage: profile-hotspots.ts <profile.cpuprofile> [top=30]");
    process.exit(1);
  }

  const profilePath = args[0];
  const topN = parseInt(args[1] ?? "30", 10);

  const profile: Profile = JSON.parse(readFileSync(profilePath, "utf-8"));

  // Aggregate hits by function+location
  const hits = new Map<string, { hits: number; fn: string; loc: string }>();

  for (const node of profile.nodes) {
    if (node.hitCount === 0) continue;

    const { functionName, url, lineNumber } = node.callFrame;

    // Skip internal/idle nodes
    if (!url || url.startsWith("node:") || functionName === "(idle)" || functionName === "(root)") {
      continue;
    }

    // Clean up the path for display
    const shortUrl = url.replace(/^file:\/\//, "").replace(process.cwd() + "/", "");
    const loc = lineNumber >= 0 ? `${shortUrl}:${lineNumber + 1}` : shortUrl;
    const fn = functionName || "(anonymous)";
    const key = `${fn}@${loc}`;

    const existing = hits.get(key);
    if (existing) {
      existing.hits += node.hitCount;
    } else {
      hits.set(key, { hits: node.hitCount, fn, loc });
    }
  }

  // Sort by hits descending
  const sorted = [...hits.values()].sort((a, b) => b.hits - a.hits);

  // Calculate total for percentages
  const totalHits = sorted.reduce((sum, x) => sum + x.hits, 0);

  console.log("\n=== CPU Profile Hotspots ===\n");
  console.log(`Total samples: ${totalHits}`);
  console.log(`Showing top ${Math.min(topN, sorted.length)} functions:\n`);

  console.log("  Hits     %   Function");
  console.log("  ─────────────────────────────────────────────────────────────");

  for (let i = 0; i < Math.min(topN, sorted.length); i++) {
    const { hits: h, fn, loc } = sorted[i];
    const pct = ((h / totalHits) * 100).toFixed(1);
    console.log(`  ${h.toString().padStart(5)}  ${pct.padStart(5)}%  ${fn}`);
    console.log(`                   ${loc}`);
  }

  console.log("");
}

main();
