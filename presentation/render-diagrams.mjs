// Wraps each diagrams/*.mmd into render/<name>.html for browser capture.
import { readFileSync, writeFileSync, readdirSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
mkdirSync(join(here, "render"), { recursive: true });

const theme = `
  body { background:#ffffff; font-family:'Segoe UI',system-ui,sans-serif; margin:0; padding:24px; }
  .wrap { display:inline-block; }
`;
for (const f of readdirSync(join(here, "diagrams"))) {
  if (!f.endsWith(".mmd")) continue;
  const src = readFileSync(join(here, "diagrams", f), "utf8")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;");
  const html = `<!DOCTYPE html><html><head><meta charset="utf-8">
<style>${theme}</style></head><body><div class="wrap"><pre class="mermaid">${src}</pre></div>
<script type="module">
  import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.esm.min.mjs";
  mermaid.initialize({
    startOnLoad: true,
    theme: "base",
    securityLevel: "loose",
    flowchart: { useMaxWidth: false, htmlLabels: true },
    sequence: { useMaxWidth: false },
    state: { useMaxWidth: false },
    pie: { useMaxWidth: false },
    themeVariables: {
      primaryColor: "#eef2f7",
      primaryBorderColor: "#2563eb",
      primaryTextColor: "#1c2733",
      lineColor: "#5f6f83",
      secondaryColor: "#eef2f7",
      tertiaryColor: "#f5f7fa",
      fontFamily: "Segoe UI, sans-serif",
      fontSize: "16px",
      pie1: "#2563eb", pie2: "#3d8bfd", pie3: "#6ea8f7", pie4: "#9ec2f9",
      pie5: "#c9dafc", pie6: "#dbe7fd", pie7: "#edf3fe", pie8: "#5f6f83",
    },
  });
</script></body></html>`;
  writeFileSync(join(here, "render", f.replace(".mmd", ".html")), html);
  console.log("render/" + f.replace(".mmd", ".html"));
}
