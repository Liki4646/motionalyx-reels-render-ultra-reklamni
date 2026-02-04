import express from "express";
import fetch from "node-fetch";
import fs from "fs";
import path from "path";
import { execFile } from "child_process";
import { promisify } from "util";
import tmp from "tmp";

const execFileAsync = promisify(execFile);
tmp.setGracefulCleanup();

const app = express();
app.use(express.json({ limit: "50mb" }));

app.get("/", (_req, res) => res.status(200).json({ ok: true }));
app.get("/health", (_req, res) => res.status(200).json({ ok: true }));

// Increase buffers so FFmpeg/FFprobe stderr doesn't crash with "maxBuffer length exceeded"
const EXEC_OPTS_FFMPEG = { maxBuffer: 1024 * 1024 * 50 }; // 50MB
const EXEC_OPTS_FFPROBE = { maxBuffer: 1024 * 1024 * 10 }; // 10MB

async function downloadToFile(url, outPath) {
  const r = await fetch(url, {
    redirect: "follow",
    headers: { "User-Agent": "Mozilla/5.0 (MotionalyxRenderBot)" }
  });
  if (!r.ok) throw new Error(`Download failed ${r.status}: ${url}`);
  const buf = Buffer.from(await r.arrayBuffer());
  fs.writeFileSync(outPath, buf);
}

function ensureArray(val, name) {
  if (!Array.isArray(val)) throw new Error(`${name} must be an array`);
}

function assEscape(t) {
  return String(t || "")
    .replace(/\r?\n/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\\/g, "\\\\")
    .replace(/{/g, "\\{")
    .replace(/}/g, "\\}");
}

function wrapByChars(text, maxCharsPerLine, maxLines) {
  const t = String(text || "").replace(/\s+/g, " ").trim();
  if (!t) return "";

  const words = t.split(" ");
  const lines = [];
  let cur = "";

  for (const w of words) {
    const cand = cur ? `${cur} ${w}` : w;

    if (cand.length <= maxCharsPerLine) {
      cur = cand;
      continue;
    }

    if (cur) lines.push(cur);
    cur = w;

    if (lines.length >= maxLines - 1) break;
  }

  if (cur && lines.length < maxLines) {
    const usedWords =
      lines.join(" ").split(" ").filter(Boolean).length +
      cur.split(" ").filter(Boolean).length;
    const remaining = words.slice(usedWords).join(" ").trim();
    if (remaining) {
      let last = cur;
      const restWords = remaining.split(" ");
      for (const w of restWords) {
        const cand = `${last} ${w}`;
        if (cand.length <= maxCharsPerLine) last = cand;
        else break;
      }
      cur = last;
    }
    lines.push(cur);
  }

  if (!lines.length) return t;

  return lines.join("\\N");
}

function normalizeAndScaleCaptions(captions, audioMs) {
  if (!Array.isArray(captions)) return [];

  const items = [];
  for (const c of captions) {
    const start = Number(c?.start_ms);
    const end = Number(c?.end_ms);
    const txt = String(c?.text || "").trim();
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start || !txt) continue;
    items.push({ dur_ms: end - start, text: txt });
  }
  if (items.length === 0) return [];

  if (!Number.isFinite(audioMs) || audioMs <= 0) {
    const out = [];
    let cur = 0;
    for (const it of items) {
      const dur = Math.max(1, Math.round(it.dur_ms));
      out.push({ start_ms: cur, end_ms: cur + dur, text: it.text });
      cur += dur;
    }
    return out;
  }

  const totalDur = items.reduce((acc, it) => acc + Math.max(1, Math.round(it.dur_ms)), 0);
  if (totalDur <= 0) return [];

  const factor = audioMs / totalDur;

  const n = items.length;
  let minSegMs = 600;
  const maxPossibleMin = Math.floor(audioMs / n);
  if (maxPossibleMin <= 0) minSegMs = 80;
  else if (minSegMs > maxPossibleMin) minSegMs = Math.max(80, maxPossibleMin);

  const scaled = items.map((it) => {
    const d = Math.max(1, Math.round(it.dur_ms));
    return { text: it.text, dur_ms: Math.max(minSegMs, Math.round(d * factor)) };
  });

  let sum = scaled.reduce((acc, x) => acc + x.dur_ms, 0);

  if (sum > audioMs) {
    let over = sum - audioMs;
    const idx = scaled
      .map((x, i) => ({ i, d: x.dur_ms }))
      .sort((a, b) => b.d - a.d)
      .map((x) => x.i);

    for (const i of idx) {
      if (over <= 0) break;
      const canReduce = Math.max(0, scaled[i].dur_ms - minSegMs);
      const reduceBy = Math.min(canReduce, over);
      scaled[i].dur_ms -= reduceBy;
      over -= reduceBy;
    }

    if (over > 0) {
      for (let i = scaled.length - 1; i >= 0 && over > 0; i--) {
        const canReduce = Math.max(0, scaled[i].dur_ms - 1);
        const reduceBy = Math.min(canReduce, over);
        scaled[i].dur_ms -= reduceBy;
        over -= reduceBy;
      }
    }
  } else if (sum < audioMs) {
    scaled[scaled.length - 1].dur_ms += audioMs - sum;
  }

  const out = [];
  let cur = 0;
  for (let i = 0; i < scaled.length; i++) {
    const dur = Math.max(1, Math.round(scaled[i].dur_ms));
    const start_ms = cur;
    const end_ms = i === scaled.length - 1 ? audioMs : start_ms + dur;
    out.push({ start_ms, end_ms, text: scaled[i].text });
    cur = end_ms;
  }
  if (out.length) out[out.length - 1].end_ms = audioMs;

  return out;
}

app.post("/render", async (req, res) => {
  const {
    audio_url,
    images,
    captions,
    end_card_url,
    end_card_duration_ms = 4000,
    end_card_audio_url,
    video = { width: 1080, height: 1920, fps: 30 }
  } = req.body || {};

  try {
    if (!audio_url) throw new Error("audio_url is required");
    if (!end_card_url) throw new Error("end_card_url is required");
    ensureArray(images, "images");
    if (images.length !== 4) throw new Error("images must have exactly 4 URLs");
    ensureArray(captions, "captions");

    const workDir = tmp.dirSync({ unsafeCleanup: true }).name;

    const img1Path = path.join(workDir, "img1.png");
    const img2Path = path.join(workDir, "img2.png");
    const img3Path = path.join(workDir, "img3.png");
    const img4Path = path.join(workDir, "img4.png");
    const endPath = path.join(workDir, "end.png");

    const audioPath = path.join(workDir, "audio.mp3");
    const assPath = path.join(workDir, "subs.ass");
    const outPath = path.join(workDir, "out.mp4");

    const sloganMp3Path = path.join(workDir, "end_card_audio.mp3");
    const sloganWavPath = path.join(workDir, "end_card_audio.wav");

    const sloganUrl = (end_card_audio_url && String(end_card_audio_url).trim()) || "";
    let hasEndCardAudio = Boolean(sloganUrl);

    console.log("[render] has end_card_audio_url:", hasEndCardAudio ? "YES" : "NO");
    if (hasEndCardAudio) console.log("[render] end_card_audio_url:", sloganUrl);

    await downloadToFile(images[0], img1Path);
    await downloadToFile(images[1], img2Path);
    await downloadToFile(images[2], img3Path);
    await downloadToFile(images[3], img4Path);
    await downloadToFile(end_card_url, endPath);
    await downloadToFile(audio_url, audioPath);

    if (hasEndCardAudio) {
      await downloadToFile(sloganUrl, sloganMp3Path);

      const mp3Size = fs.existsSync(sloganMp3Path) ? fs.statSync(sloganMp3Path).size : 0;
      console.log("[render] slogan mp3 bytes:", mp3Size);

      if (mp3Size < 1500) {
        console.log("[render] slogan mp3 seems too small -> skipping end card audio");
        hasEndCardAudio = false;
      } else {
        try {
          await execFileAsync(
            "ffmpeg",
            [
              "-y",
              "-hide_banner",
              "-loglevel",
              "error",
              "-nostdin",
              "-i",
              sloganMp3Path,
              "-ac",
              "1",
              "-ar",
              "24000",
              "-c:a",
              "pcm_s16le",
              sloganWavPath
            ],
            EXEC_OPTS_FFMPEG
          );

          const wavSize = fs.existsSync(sloganWavPath) ? fs.statSync(sloganWavPath).size : 0;
          console.log("[render] slogan wav bytes:", wavSize);

          if (wavSize < 3000) {
            console.log("[render] slogan wav too small -> skipping end card audio");
            hasEndCardAudio = false;
          }
        } catch (e) {
          console.log("[render] slogan re-encode failed -> skipping end card audio:", String(e?.message || e));
          hasEndCardAudio = false;
        }
      }
    }

    // Probe main audio duration
    let audioMs = NaN;
    try {
      const { stdout: probeOut } = await execFileAsync(
        "ffprobe",
        [
          "-v",
          "error",
          "-show_entries",
          "format=duration",
          "-of",
          "default=noprint_wrappers=1:nokey=1",
          audioPath
        ],
        EXEC_OPTS_FFPROBE
      );
      const audioSeconds = parseFloat(String(probeOut || "").trim());
      if (Number.isFinite(audioSeconds) && audioSeconds > 0) audioMs = Math.round(audioSeconds * 1000);
    } catch (_e) {
      audioMs = NaN;
    }

    const w = Number(video.width || 1080);
    const h = Number(video.height || 1920);
    const fps = Number(video.fps || 30);

    const scaledCaptions = normalizeAndScaleCaptions(captions, audioMs);

    // If probe failed, use last caption end as “audio”
    let effectiveAudioMs = audioMs;
    if (!Number.isFinite(effectiveAudioMs) || effectiveAudioMs <= 0) {
      const lastEnd = scaledCaptions.length ? Number(scaledCaptions[scaledCaptions.length - 1].end_ms) : 0;
      effectiveAudioMs = Number.isFinite(lastEnd) && lastEnd > 0 ? Math.round(lastEnd) : 15000;
    }

    // =============================
    // SLIDES TIMING (SEGMENT-DRIVEN)
    // 7 segments:
    // - img1 = seg1
    // - img2 = seg2+seg3
    // - img3 = seg4+seg5
    // - img4 = seg6+seg7
    // =============================
    let seg1 = 0,
      seg2 = 0,
      seg3 = 0,
      seg4 = 0;

    if (scaledCaptions.length >= 7) {
      const t1 = Math.round(Number(scaledCaptions[0].end_ms)); // end seg1
      const t3 = Math.round(Number(scaledCaptions[2].end_ms)); // end seg3
      const t5 = Math.round(Number(scaledCaptions[4].end_ms)); // end seg5
      const t7 = Math.round(Number(scaledCaptions[6].end_ms)); // end seg7

      seg1 = Math.max(1, t1);
      seg2 = Math.max(1, t3 - t1);
      seg3 = Math.max(1, t5 - t3);
      seg4 = Math.max(1, t7 - t5);

      effectiveAudioMs = Math.max(effectiveAudioMs, t7);
    } else {
      // Fallback: split into 4 equal parts if input is malformed
      const part = Math.floor(effectiveAudioMs / 4);
      seg1 = Math.max(1, part);
      seg2 = Math.max(1, part);
      seg3 = Math.max(1, part);
      seg4 = Math.max(1, effectiveAudioMs - seg1 - seg2 - seg3);
    }

    const slideshowMs = Math.max(1, Math.round(seg1 + seg2 + seg3 + seg4));
    const endCardDurMs = Math.max(0, Math.round(Number(end_card_duration_ms) || 0));
    const totalMs = slideshowMs + endCardDurMs;

    const coverCrop = `scale=${w}:${h}:force_original_aspect_ratio=increase,crop=${w}:${h},setsar=1,fps=${fps},format=yuv420p`;

    // SUBTITLES (ASS)
    const titleFontSize = 100;
    const titleOutline = 3;

    const captionFontSize = 100;
    const captionOutline = 3;

    const marginLR = Math.round(w * 0.10);

    // Center all subtitles on screen
    const marginV = 0;
    const titleMarginV = 0;

    // Slide-in params (B)
    const capX = Math.round(w / 2);

    // UNIVERSAL SAFE (IG Reels + YT Shorts) - move captions higher to avoid UI overlays
    // capY represents the bottom of the text block (Alignment=2)
    const SAFE_BOTTOM = Math.round(h * 0.32); // ~614px for 1920 height
    const capY = h - SAFE_BOTTOM; // ~1306 for 1920 height

    const slideDy = Math.max(20, Math.round(h * 0.035)); // ~3.5% of height
    const slideInMs = 220;
    const fadeInMs = 120;
    const fadeOutMs = 120;

    const titleMaxCharsPerLine = 12;
    const titleMaxLines = 6;

    const capMaxCharsPerLine = 18;
    const capMaxLines = 5;

    const header = `[Script Info]
ScriptType: v4.00+
PlayResX: ${w}
PlayResY: ${h}
WrapStyle: 2
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Title,DejaVu Sans,${titleFontSize},&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,${titleOutline},0,5,${marginLR},${marginLR},${titleMarginV},1
Style: Caption,DejaVu Sans,${captionFontSize},&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,${captionOutline},0,5,${marginLR},${marginLR},${marginV},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
`;

    function msToAssTime(ms) {
      const t = Math.max(0, Number(ms) || 0);
      const cs = Math.floor(t / 10);
      const hh = Math.floor(cs / 360000);
      const mm = Math.floor((cs % 360000) / 6000);
      const ss = Math.floor((cs % 6000) / 100);
      const cc = cs % 100;
      const pad2 = (n) => String(n).padStart(2, "0");
      return `${hh}:${pad2(mm)}:${pad2(ss)}.${pad2(cc)}`;
    }

    // Slide-in override (from slightly below -> target Y), plus fade
    // \move(x1,y1,x2,y2,t1,t2) is in ms relative to line start
    const slideTag = `{\\move(${capX},${capY + slideDy},${capX},${capY},0,${slideInMs})\\fad(${fadeInMs},${fadeOutMs})}`;

    let ass = header;

    for (let i = 0; i < scaledCaptions.length; i++) {
      const c = scaledCaptions[i];
      const start = msToAssTime(c.start_ms);
      const end = msToAssTime(c.end_ms);

      if (i === 0) {
        const raw = assEscape(c.text);
        const wrapped = wrapByChars(raw, titleMaxCharsPerLine, titleMaxLines);
        ass += `Dialogue: 0,${start},${end},Title,,0,0,0,,${slideTag}${wrapped}\n`;
      } else {
        const raw = assEscape(c.text);
        const wrapped = wrapByChars(raw, capMaxCharsPerLine, capMaxLines);
        ass += `Dialogue: 0,${start},${end},Caption,,0,0,0,,${slideTag}${wrapped}\n`;
      }
    }

    fs.writeFileSync(assPath, ass, "utf8");

    // VIDEO FILTER (Classic dissolve crossfade)
    const xfadeDur = 0.30;

    // Make sure each segment can accommodate the fade (avoid negative offsets)
    const safeSeg1 = Math.max(seg1, Math.ceil(xfadeDur * 1000) + 1);
    const safeSeg2 = Math.max(seg2, Math.ceil(xfadeDur * 1000) + 1);
    const safeSeg3 = Math.max(seg3, Math.ceil(xfadeDur * 1000) + 1);
    const safeSeg4 = Math.max(seg4, 1);

    const off1 = Math.max(0.001, safeSeg1 / 1000 - xfadeDur);
    const off2 = Math.max(0.001, (safeSeg1 + safeSeg2) / 1000 - 2 * xfadeDur);
    const off3 = Math.max(0.001, (safeSeg1 + safeSeg2 + safeSeg3) / 1000 - 3 * xfadeDur);

    const filterParts = [
      `[0:v]${coverCrop}[v0]`,
      `[1:v]${coverCrop}[v1]`,
      `[2:v]${coverCrop}[v2]`,
      `[3:v]${coverCrop}[v3]`,
      `[4:v]${coverCrop}[v4]`,

      `[v0]trim=duration=${(safeSeg1 / 1000).toFixed(3)},setpts=PTS-STARTPTS[s0]`,
      `[v1]trim=duration=${(safeSeg2 / 1000).toFixed(3)},setpts=PTS-STARTPTS[s1]`,
      `[v2]trim=duration=${(safeSeg3 / 1000).toFixed(3)},setpts=PTS-STARTPTS[s2]`,
      `[v3]trim=duration=${(safeSeg4 / 1000).toFixed(3)},setpts=PTS-STARTPTS[s3]`,

      `[s0][s1]xfade=transition=fade:duration=${xfadeDur.toFixed(2)}:offset=${off1.toFixed(3)}[x01]`,
      `[x01][s2]xfade=transition=fade:duration=${xfadeDur.toFixed(2)}:offset=${off2.toFixed(3)}[x012]`,
      `[x012][s3]xfade=transition=fade:duration=${xfadeDur.toFixed(2)}:offset=${off3.toFixed(3)}[slideshow]`,

      `[slideshow]ass=${assPath.replace(/\\/g, "\\\\")}[subbed]`,

      `[v4]trim=duration=${(endCardDurMs / 1000).toFixed(3)},setpts=PTS-STARTPTS[endcard]`,
      `[subbed][endcard]concat=n=2:v=1:a=0[vout]`
    ];

    // AUDIO
    const endCardStartSec = slideshowMs / 1000;
    const totalDurSec = totalMs / 1000;

    const sloganStartSec = endCardStartSec + 0.2;
    const sloganDelayMs = Math.max(0, Math.round(sloganStartSec * 1000));

    filterParts.push(
      `[5:a]asetpts=PTS-STARTPTS,` +
        `atrim=0:${endCardStartSec.toFixed(3)},` +
        `apad=pad_dur=${(endCardDurMs / 1000 + 2).toFixed(3)},` +
        `atrim=0:${totalDurSec.toFixed(3)}` +
        `[amain]`
    );

    if (hasEndCardAudio) {
      const fadeIn = 0.12;

      filterParts.push(
        `[6:a]asetpts=PTS-STARTPTS,` +
          `afade=t=in:st=0:d=${fadeIn},` +
          `volume=1.35,` +
          `adelay=${sloganDelayMs}|${sloganDelayMs}` +
          `[aslogan]`,
        `[amain][aslogan]amix=inputs=2:duration=longest:normalize=0[aout]`,
        `[aout]atrim=0:${totalDurSec.toFixed(3)}[aout2]`
      );
    } else {
      filterParts.push(`[amain]atrim=0:${totalDurSec.toFixed(3)}[aout2]`);
    }

    const filter = filterParts.join(";");

    const args = [
      "-y",
      "-hide_banner",
      "-loglevel",
      "error",
      "-nostdin",

      "-loop",
      "1",
      "-t",
      (safeSeg1 / 1000).toFixed(3),
      "-i",
      img1Path,

      "-loop",
      "1",
      "-t",
      (safeSeg2 / 1000).toFixed(3),
      "-i",
      img2Path,

      "-loop",
      "1",
      "-t",
      (safeSeg3 / 1000).toFixed(3),
      "-i",
      img3Path,

      "-loop",
      "1",
      "-t",
      (safeSeg4 / 1000).toFixed(3),
      "-i",
      img4Path,

      "-loop",
      "1",
      "-t",
      (endCardDurMs / 1000).toFixed(3),
      "-i",
      endPath,

      "-i",
      audioPath
    ];

    if (hasEndCardAudio) {
      args.push("-i", sloganWavPath);
    }

    args.push(
      "-filter_complex",
      filter,

      "-map",
      "[vout]",
      "-map",
      "[aout2]",

      "-r",
      String(fps),

      "-c:v",
      "libx264",
      "-pix_fmt",
      "yuv420p",
      "-profile:v",
      "high",
      "-level",
      "4.1",

      "-c:a",
      "aac",
      "-b:a",
      "192k",

      outPath
    );

    console.log("[render] ffmpeg starting...");

    await execFileAsync("ffmpeg", args, EXEC_OPTS_FFMPEG);

    const mp4 = fs.readFileSync(outPath);
    res.setHeader("Content-Type", "video/mp4");
    res.status(200).send(mp4);
  } catch (err) {
    res.status(400).json({ ok: false, error: String(err?.message || err) });
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`listening on ${port}`));
