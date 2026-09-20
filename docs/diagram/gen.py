import json, sys, itertools

FONT = 5  # Excalifont
LINE = 1.25
els, ids = [], itertools.count(1)

def base(kind, x, y, w, h, stroke, bg="transparent", **kw):
    e = dict(id=f"e{next(ids)}", type=kind, x=x, y=y, width=w, height=h, angle=0,
             strokeColor=stroke, backgroundColor=bg, fillStyle="solid", strokeWidth=2,
             strokeStyle="solid", roughness=1, opacity=100, groupIds=[], frameId=None,
             roundness=None, seed=next(ids) * 7919, version=1, versionNonce=1, isDeleted=False,
             boundElements=None, updated=1, link=None, locked=False)
    e.update(kw)
    els.append(e)
    return e

def text(x, y, w, s, size=18, color="#1e1e1e", align="center"):
    lines = s.split("\n")
    h = len(lines) * size * LINE
    base("text", x, y, w, h, color, text=s, fontSize=size, fontFamily=FONT, textAlign=align,
         verticalAlign="top", containerId=None, originalText=s, autoResize=False, lineHeight=LINE)
    return h

def box(x, y, w, h, s, stroke, bg, size=18):
    base("rectangle", x, y, w, h, stroke, bg)
    n = len(s.split("\n"))
    text(x + 6, y + (h - n * size * LINE) / 2, w - 12, s, size, stroke)

def zone(x, y, w, h, label, stroke, bg):
    base("rectangle", x, y, w, h, stroke, bg, strokeStyle="dashed", opacity=60)
    text(x + 18, y + 12, w - 36, label, 18, stroke, "left")

def arrow(pts, stroke, dashed=False, label=None, both=False):
    x0, y0 = pts[0]
    rel = [[px - x0, py - y0] for px, py in pts]
    xs, ys = [p[0] for p in rel], [p[1] for p in rel]
    base("arrow", x0, y0, max(xs) - min(xs), max(ys) - min(ys), stroke, points=rel,
         strokeStyle="dashed" if dashed else "solid", startBinding=None, endBinding=None,
         startArrowhead="arrow" if both else None, endArrowhead="arrow", lastCommittedPoint=None,
         roundness={"type": 2}, elbowed=False)
    if label:
        (ax, ay), (bx, by) = pts[0], pts[1]
        text((ax + bx) / 2 - 70, (ay + by) / 2 - 24, 140, label, 15, stroke)

BLUE, BLUE_BG = "#1971c2", "#a5d8ff"
GREEN, GREEN_BG = "#2f9e44", "#b2f2bb"
TEAL, TEAL_BG = "#0c8599", "#99e9f2"
ORANGE, ORANGE_BG = "#e8590c", "#ffd8a8"
RED, RED_BG = "#e03131", "#ffc9c9"
VIOLET, VIOLET_BG = "#6741d9", "#d0bfff"
YELLOW, YELLOW_BG = "#f08c00", "#ffec99"
GREY = "#343a40"

text(10, 8, 600, "walcast: architecture", 30, "#1e1e1e", "left")

PX, PW = 40, 250          # postgres boxes
GX = 520                  # stage 1 boxes start
GW = 590

# PostgreSQL
zone(10, 80, 310, 830, "PostgreSQL", BLUE, "#e7f5ff")
box(PX, 134, PW, 64, "publication\nwalcast_pub", BLUE, BLUE_BG)
box(PX, 250, PW, 86, "slot walcast_slot\npgoutput ·\nconfirmed_flush_lsn", BLUE, BLUE_BG, 16)
box(PX, 372, PW, 86, "WAL · wal_level=logical\nrow changes +\nlogical messages", BLUE, BLUE_BG, 16)
text(PX, 466, PW, "marker H = pg_logical_emit_message,\ndecoded at its WAL position", 13, BLUE, "left")
box(PX, 640, PW, 86, "tables\nchunk SELECT by primary key\nREPEATABLE READ", BLUE, BLUE_BG, 16)
box(PX, 770, PW, 108, "walcast_state\nslots: existed, generation\nbackfills: table, last key,\nstatus", BLUE, BLUE_BG, 16)
text(PX, 520, PW, "the slot holds the position; the\nstate table only remembers that\nthe slot existed and how far\neach backfill got", 13, BLUE, "left")

# process
zone(470, 80, 1400, 830, "walcast process", GREY, "#f8f9fa")
box(GX, 124, 1320, 84,
    "Supervisor: validate → state table → slot decision (keep · adopt · create · recreate once · refuse) → START_REPLICATION, messages on\n"
    "retry: backoff + jitter  ·  fatal, no retry: slot lost or unusable, state store unusable, sink rejected, sink stuck\n"
    "SIGTERM: stop admission, drain with deadline, ack, close",
    YELLOW, YELLOW_BG, 15)

zone(495, 228, 640, 664, "Stage 1 · source", GREEN, "#ebfbee")
box(GX, 270, GW, 64, "feedback: 5s tick + keepalive reply\nidle: ack walEnd only when ledger is empty", RED, RED_BG, 16)
box(GX, 384, GW, 64, "WAL receiver · owner loop, never blocks\nrecv deadline · selectable enqueue", GREEN, GREEN_BG, 16)
box(GX, 478, GW, 64, "typed codec (OID-aware) → pooled Batch\nTOAST unchanged · key-change split · state tables ignored", GREEN, GREEN_BG, 16)
box(GX, 572, GW, 86, "key tracker · only for the table being backfilled\nkey → xid of its last decoded change, complete image or patch\nkey move with a missing TOAST column: hold the ack before it", TEAL, TEAL_BG, 16)
box(GX, 688, GW, 86, "chunk merge · runs when marker H is decoded · key touched by xid ≥ chunk xmin:\ncomplete image in the stream → drop · only a patch → read the row again\nappend the rest as op \"read\" · batch ack LSN = H", TEAL, TEAL_BG, 15)
box(GX, 808, GW, 64, "backfill worker · 2nd connection · one chunk in flight · exact re-reads\nreads only once snapshot xmin ≥ fence xid taken after tracking began", TEAL, TEAL_BG, 15)
arrow([(815, 448), (815, 478)], GREEN)
arrow([(815, 542), (815, 572)], TEAL)
arrow([(815, 658), (815, 688)], TEAL)
arrow([(815, 808), (815, 774)], TEAL)
text(825, 780, 280, "rows, xmin ↑  ↓ delivered, re-read keys", 13, TEAL, "left")

zone(1165, 228, 680, 664, "Stage 2 · delivery", ORANGE, "#fff4e6")
SX, SW = 1190, 630
box(SX, 270, SW, 86, "Tx ledger · delivered LSN moves only across contiguous done batches\nack = end LSN of the last whole transaction · fragments carry none\nmarker H is acked like a commit · reported LSN = delivered, capped by a hold", RED, RED_BG, 15)
box(SX, 467, 400, 86, "byte-bounded queue\nbackpressure", VIOLET, VIOLET_BG, 16)
box(SX, 620, SW, 86, "Dispatcher · one Send at a time · callbacks awaited\nretry in place · rejected = halt", ORANGE, ORANGE_BG, 16)
box(SX, 770, SW, 86, "Metrics · /metrics + pprof\nlag · slot health · sink p99 · ledger depth · backfill rows", GREY, "#e9ecef", 16)
arrow([(GX + GW, 510), (SX, 510)], VIOLET)
arrow([(1390, 553), (1390, 620)], VIOLET)
arrow([(1720, 620), (1720, 356)], RED)
text(1730, 470, 110, "done(err)", 13, RED, "left")
arrow([(SX, 302), (GX + GW, 302)], RED, dashed=True)
text(1120, 306, 70, "reported", 13, RED, "left")

# sinks
zone(1950, 80, 300, 830, "Sinks (one interface)", VIOLET, "#f3f0ff")
text(1975, 124, 260, "Send(ctx, *Batch, done func(error))\ndone exactly once\nevent id = commit LSN + seq", 14, VIOLET, "left")
box(1980, 470, 240, 86, "stdout · JSON lines\nprivate copy per write", VIOLET, VIOLET_BG, 16)
box(1980, 620, 240, 86, "webhook · POST batch\nHMAC · idempotency key\nretry forever", VIOLET, VIOLET_BG, 16)
box(1980, 770, 240, 86, "Kafka · 1 record/event\ntopic/table · key = identity\nidempotent · acks=all", VIOLET, VIOLET_BG, 16)
arrow([(SX + SW, 640), (1980, 513)], ORANGE)
arrow([(SX + SW, 663), (1980, 663)], ORANGE)
arrow([(SX + SW, 686), (1980, 813)], ORANGE)

# pg <-> process: every link is one short straight line
arrow([(GX, 166), (PX + PW, 166)], YELLOW, both=True)
text(330, 136, 170, "create if missing", 13, YELLOW, "left")
arrow([(GX, 302), (PX + PW, 293)], RED, dashed=True)
text(345, 268, 120, "ack LSN", 13, RED, "left")
arrow([(PX + PW, 415), (GX, 415)], BLUE)
text(330, 386, 170, "changes + markers", 13, BLUE, "left")
arrow([(GX, 826), (PX + PW, 683)], TEAL, both=True)
text(322, 690, 110, "chunks", 13, TEAL, "left")
arrow([(GX, 852), (PX + PW, 836)], TEAL, both=True)
text(330, 856, 180, "progress, emit H", 13, TEAL, "left")

doc = {"type": "excalidraw", "version": 2, "source": "walcast/docs", "elements": els,
       "appState": {"viewBackgroundColor": "#ffffff", "gridSize": None}, "files": {}}
json.dump(doc, open(sys.argv[1], "w"), indent=1)
print(len(els), "elements")
