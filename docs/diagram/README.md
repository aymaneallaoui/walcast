# Architecture diagram

`docs/architecture.excalidraw` is the source and opens in Excalidraw. It is generated, so edit `gen.py`, not the file.

```
python3 docs/diagram/gen.py docs/architecture.excalidraw
npm install --no-save playwright-core
CHROME=/path/to/chrome node docs/diagram/render.mjs docs/architecture.excalidraw docs/architecture.png
```

`render.mjs` loads Excalidraw's own `exportToSvg` in headless Chromium, so the image matches what the editor shows.
