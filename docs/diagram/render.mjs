import { chromium } from 'playwright-core';
import { readFileSync, writeFileSync } from 'node:fs';
const [src, out] = process.argv.slice(2);
const scene = JSON.parse(readFileSync(src, 'utf8'));
const browser = await chromium.launch({ executablePath: process.env.CHROME, args: ['--no-sandbox'] });
const page = await browser.newPage({ deviceScaleFactor: 1 });
page.on('pageerror', e => console.error('pageerror:', e.message));
await page.setContent('<!doctype html><html><body style="margin:0;background:#fff"><div id="out"></div></body></html>');
await page.addScriptTag({ type: 'module', content: `
  import { exportToSvg } from 'https://esm.sh/@excalidraw/excalidraw@0.18.0?bundle';
  window.__render = async (scene) => {
    const svg = await exportToSvg({ elements: scene.elements, appState: { ...scene.appState, exportBackground: true, exportPadding: 16, exportScale: 1 }, files: {} });
    document.getElementById('out').appendChild(svg);
    await document.fonts.ready;
    return [svg.width.baseVal.value, svg.height.baseVal.value];
  };
  window.__ready = true;` });
await page.waitForFunction('window.__ready === true', null, { timeout: 90000 });
const [w, h] = await page.evaluate(s => window.__render(s), scene);
await page.waitForTimeout(1500);
await page.setViewportSize({ width: Math.ceil(w), height: Math.ceil(h) });
writeFileSync(out, await (await page.$('#out svg')).screenshot({ type: 'png' }));
console.log('rendered', Math.ceil(w), 'x', Math.ceil(h));
await browser.close();
