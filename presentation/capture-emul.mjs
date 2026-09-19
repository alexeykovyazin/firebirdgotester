// Captures the live OLTPEMUL tab from the running UI into assets/ui_oltpemul.png
import puppeteer from "puppeteer";

const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
const page = await browser.newPage();
await page.setViewport({ width: 1760, height: 980, deviceScaleFactor: 1.4 });
await page.goto("http://127.0.0.1:9000/", { waitUntil: "networkidle2", timeout: 30000 });
await page.click('button[data-tab="oltpemul"]');
await page.click("#btnEmulRefresh");
await new Promise((r) => setTimeout(r, 4500));
await page.screenshot({ path: "assets/ui_oltpemul.png", fullPage: false });
await browser.close();
console.log("OK assets/ui_oltpemul.png");
