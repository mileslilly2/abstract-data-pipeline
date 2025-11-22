// ───────────────────────────────────────────────
// Music-Reactive Mandala V2 + WAV Playback
// - MIDI JSON event integration
// - amplitude CSV
// - bass/mid/treble energy
// - spin / shockwave / supernova / constellations
// - mirrored layers
// - motion blur trails
// - safe frame saving
// - full WAV playback (Minim)
// Miles + robot, 2025
// ───────────────────────────────────────────────

import java.io.File;
import java.util.ArrayList;
import processing.data.*;

// AUDIO PLAYBACK
import ddf.minim.*;
Minim minim;
AudioPlayer player;

// ----- Mandala + render control -----
int symmetry = 12;
int layers   = 10;
float maxR;

float fps        = 30.0;
int   saveEvery  = 2;

int frameCounter = 0;
String frameFolder = "frames";

float totalDuration = 15.0;
int   totalFrames   = int(totalDuration * fps);
float progress = 0;

// ----- Music data -----
JSONArray midiEvents;
float audioDuration = 0;

Table ampTable;
int   ampIndex = 0;

// global features
boolean hasNote = false;
float noteEnergy     = 0;
float notePitchNorm  = 0;
float ampNorm        = 0;

float prevAmpNorm    = 0;
float prevNoteEnergy = 0;

// band energies
float bassEnergy   = 0;
float midEnergy    = 0;
float trebleEnergy = 0;

ArrayList<JSONObject> activeNotes = new ArrayList<JSONObject>();

// pitch normalization range
int minPitch = 30;
int maxPitch = 80;

// ----- PATHS -----
String midiJsonPath = "/home/miles/Documents/abstract-data-pipeline/plugins/midi/data/json/104_funk-rock_92_fill_4-4.json";
String ampCsvPath   = "/home/miles/Documents/abstract-data-pipeline/plugins/midi/data/csv/104_funk-rock_92_fill_4-4.csv";
String wavPath      = "/home/miles/Documents/abstract-data-pipeline/plugins/midi/data/wav/104_funk-rock_92_fill_4-4.wav";

// palette
color[] palette;

// noise cache
float[] noiseBuf = new float[128];

float prevAmpForSuper = 0;

// ───────────────────────────────────────────────
// SETUP
// ───────────────────────────────────────────────
void setup() {
  size(1024, 1024, P2D);
  smooth(4);
  frameRate(fps);

  colorMode(HSB, 360, 100, 100, 100);

  maxR = width * 0.45;

  palette = new color[]{
    color(20, 80, 100),
    color(200, 70, 100),
    color(50, 90, 100),
    color(160, 60, 100),
    color(310, 70, 95)
  };

  background(0, 0, 12);

  // frames folder
  File dir = new File(sketchPath(frameFolder));
  if (!dir.exists()) dir.mkdirs();

  // load MIDI
  JSONObject midiJson = loadJSONObject(midiJsonPath);
  midiEvents   = midiJson.getJSONArray("events");
  audioDuration = midiJson.getFloat("length_seconds");

  // load amplitude CSV
  ampTable = loadTable(ampCsvPath, "header,csv");

  println("Loaded MIDI events: " + midiEvents.size());
  println("Loaded amplitude samples: " + ampTable.getRowCount());

  // load WAV
  minim = new Minim(this);
  player = minim.loadFile(wavPath);
  player.play();
  player.cue(0);

  println("Loaded WAV: " + wavPath);
}

// ───────────────────────────────────────────────
// DRAW
// ───────────────────────────────────────────────
void draw() {

  if (frameCounter >= totalFrames) {
    println("Finished 15s render.");

    if (player != null) player.close();
    if (minim != null) minim.stop();

    noLoop();
    return;
  }

  // motion blur / afterimage fade
  noStroke();
  fill(0, 0, 8, 7);
  rect(0, 0, width, height);

  progress = frameCounter / float(totalFrames);
  float audioTime = progress * audioDuration;

  updateAmplitude(audioTime);
  updateActiveNotes(audioTime);

  float totalSlices   = layers * symmetry;
  float sliceProgress = progress * totalSlices;

  int currentSliceIndex = int(sliceProgress);
  if (currentSliceIndex >= totalSlices) {
    noLoop();
    return;
  }
  float localProg = sliceProgress - currentSliceIndex;

  int currentLayer = currentSliceIndex / symmetry;
  int currentSlice = currentSliceIndex % symmetry;

  float innerR = map(currentLayer,   0, layers, 20, maxR * 0.95);
  float outerR = map(currentLayer+1, 0, layers, 20, maxR);

  float ampScale  = 1.0 + ampNorm * 0.4;
  float noteScale = 1.0 + noteEnergy * 0.3;
  float bandScale = 1.0 + bassEnergy * 0.25;

  float totalScale = ampScale * noteScale * bandScale;

  innerR *= totalScale;
  outerR *= totalScale;

  // global spin
  float spin = map(notePitchNorm, 0, 1, -0.015, 0.015);

  pushMatrix();
  translate(width/2, height/2);
  rotate(spin * frameCount);

  // main slice
  drawSliceFast(innerR, outerR, currentLayer, currentSlice, localProg);

  // mirrored layer
  pushMatrix();
  scale(0.75);
  rotate(-spin * frameCount * 0.7);
  drawSliceFast(innerR * 0.85, outerR * 0.85, currentLayer, currentSlice, localProg);
  popMatrix();

  // shockwave
  if (noteEnergy > 0.6) drawShockwave();

  // supernova
  if (ampNorm > 0.8 && prevAmpForSuper <= 0.8) drawSupernova();

  // constellations
  drawConstellations();

  popMatrix();

  prevAmpForSuper = ampNorm;

  // save frame
  if (frameCounter % saveEvery == 0) {
    save(frameFolder + "/frame_" + nf(frameCounter, 5) + ".png");
  }

  frameCounter++;
}

// ───────────────────────────────────────────────
// amplitude
// ───────────────────────────────────────────────
void updateAmplitude(float t) {
  int n = ampTable.getRowCount();

  while (ampIndex < n - 1 && ampTable.getFloat(ampIndex, "time") < t) {
    ampIndex++;
  }

  float a   = ampTable.getFloat(ampIndex, "amplitude");
  float raw = map(a, -0.03, 0.03, 0, 1);
  raw       = constrain(raw, 0, 1);

  ampNorm = lerp(prevAmpNorm, raw, 0.25);
  prevAmpNorm = ampNorm;
}

// ───────────────────────────────────────────────
// MIDI + band energies
// ───────────────────────────────────────────────
void updateActiveNotes(float t) {
  float window = 0.03;

  activeNotes.clear();
  bassEnergy = midEnergy = trebleEnergy = 0;

  float velSum = 0;
  float pitchSum = 0;
  int count = 0;

  for (int i = 0; i < midiEvents.size(); i++) {
    JSONObject ev = midiEvents.getJSONObject(i);
    if (!"note_on".equals(ev.getString("type"))) continue;
    int vel = ev.getInt("velocity");
    if (vel <= 0) continue;

    float et = ev.getFloat("time");
    if (abs(et - t) > window) continue;

    activeNotes.add(ev);

    float vNorm = vel / 127.0;
    int pitch = ev.getInt("pitch");

    velSum   += vel;
    pitchSum += pitch;
    count++;

    if (pitch < 50)      bassEnergy   += vNorm;
    else if (pitch < 70) midEnergy    += vNorm;
    else                 trebleEnergy += vNorm;
  }

  bassEnergy   = constrain(bassEnergy,   0, 1);
  midEnergy    = constrain(midEnergy,    0, 1);
  trebleEnergy = constrain(trebleEnergy, 0, 1);

  if (count == 0) {
    hasNote = false;
    noteEnergy = lerp(prevNoteEnergy, 0, 0.1);
    prevNoteEnergy = noteEnergy;
    return;
  }

  hasNote = true;

  float avgVel   = velSum / count;
  float avgPitch = pitchSum / count;

  float velNorm = avgVel / 127.0;
  float pNorm   = constrain(map(avgPitch, minPitch, maxPitch, 0, 1), 0, 1);

  noteEnergy     = lerp(prevNoteEnergy, velNorm, 0.4);
  prevNoteEnergy = noteEnergy;
  notePitchNorm  = pNorm;
}

// ───────────────────────────────────────────────
// Slice drawing
// ───────────────────────────────────────────────
void drawSliceFast(float innerR, float outerR, int layer, int slice, float prog) {

  float angleStep = TWO_PI / symmetry;
  float angle = slice * angleStep;

  pushMatrix();
  rotate(angle);

  int baseMotif = (layer + slice) % 4;
  int motif = (baseMotif + int(notePitchNorm * 3)) % 4;

  switch (motif) {
    case 0: // ARC
      noFill();
      strokeWeight(1.5 + 4*noteEnergy + 3*ampNorm + 4*bassEnergy);
      stroke(pickReactiveColor());
      arc(0, 0, outerR*2, outerR*2, 0, angleStep * prog);
      break;

    case 1: // PETAL
      fill(pickReactiveColor());
      stroke(0, 0, 0, 40);
      strokeWeight(1 + 2*ampNorm + 2*midEnergy);
      beginShape();
      vertex(innerR, 0);

      float warp = 0.2 + 0.4*noteEnergy + 0.3*midEnergy;

      bezierVertex(innerR*1.1,
                   -outerR*warp*prog,
                   outerR*0.8,
                   -outerR*warp*prog,
                   outerR*prog, 0);
      bezierVertex(outerR*0.8,
                   outerR*warp*prog,
                   innerR*1.1,
                   innerR*warp*prog,
                   innerR,
                   0);
      endShape(CLOSE);
      break;

    case 2: // SPIKE
      noStroke();
      fill(pickReactiveColor());
      float rmid = lerp(innerR, outerR, prog);
      float spikeSpread = 0.25 + 0.3*ampNorm + 0.2*trebleEnergy;
      beginShape();
      vertex(innerR, 0);
      vertex(rmid, -outerR * spikeSpread * prog);
      vertex(rmid,  outerR * spikeSpread * prog);
      endShape(CLOSE);
      break;

    case 3: // NOISE WARP
      noFill();
      stroke(pickReactiveColor());
      strokeWeight(1.5 + 2*ampNorm + 2*noteEnergy + 1.5*trebleEnergy);

      int segs = max(3, int(20 * prog));
      float speed = 0.005 + ampNorm * 0.02 + trebleEnergy * 0.015;

      for (int i = 0; i < segs; i++) {
        noiseBuf[i] = noise(i * 0.25 + frameCount * speed);
      }

      float ns = 0.25 + 0.75*ampNorm + 0.3*trebleEnergy;

      beginShape();
      for (int i = 0; i < segs; i++) {
        float t = map(i, 0, segs - 1, 0, angleStep);
        float nr = lerp(innerR, outerR, noiseBuf[i] * ns);
        vertex(cos(t)*nr, sin(t)*nr);
      }
      endShape();
      break;
  }

  popMatrix();
}

// ───────────────────────────────────────────────
// Shockwave
// ───────────────────────────────────────────────
void drawShockwave() {
  float r = lerp(maxR*0.2, maxR*1.1, ampNorm);
  noFill();
  stroke(0, 0, 100, 40 + 30*trebleEnergy);
  strokeWeight(2 + 3*noteEnergy);
  ellipse(0, 0, r*2, r*2);
}

// ───────────────────────────────────────────────
// Supernova
// ───────────────────────────────────────────────
void drawSupernova() {
  int rays = symmetry * 2;
  stroke(0, 0, 100, 60);
  strokeWeight(1.5 + 2*bassEnergy);
  for (int k=0; k<rays; k++) {
    float a = TWO_PI * k / float(rays);
    float r = maxR * (0.8 + 0.4*ampNorm);
    line(0, 0, cos(a)*r, sin(a)*r);
  }
}

// ───────────────────────────────────────────────
// Constellations
// ───────────────────────────────────────────────
void drawConstellations() {
  noStroke();
  for (JSONObject n : activeNotes) {
    float pitch = n.getInt("pitch");
    float vNorm = n.getInt("velocity") / 127.0;

    float ang = map(pitch, minPitch, maxPitch, 0, TWO_PI);

    float baseR = maxR * 0.25 + bassEnergy * maxR * 0.15;
    float rr = baseR + vNorm * maxR * 0.3;

    float x = cos(ang) * rr;
    float y = sin(ang) * rr;

    float starSize = 3 + 5*vNorm + 3*trebleEnergy;

    fill(0, 0, 100, 50 + 40*vNorm);
    ellipse(x, y, starSize, starSize);
  }
}

// ───────────────────────────────────────────────
// Color utilities
// ───────────────────────────────────────────────
color pickReactiveColor() {
  int base = int(random(palette.length));
  int shift = int(notePitchNorm * 60 - 30);

  color c = palette[base];
  c = shiftHue(c, shift);

  float boost = constrain(0.4*ampNorm + 0.6*noteEnergy + 0.3*midEnergy, 0, 1);
  color bright = color(hue(c), saturation(c), 100, alpha(c));

  return lerpColor(c, bright, boost);
}

color shiftHue(color c, float delta) {
  float h = (hue(c) + delta + 360) % 360;
  float s = saturation(c);
  float b = brightness(c);
  float a = alpha(c);
  return color(h, s, b, a);
}
