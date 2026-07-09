#!/usr/bin/env python3
"""
pollution.py -- Decode the Foobot's raw sensor readings and rebuild the global
pollution index ("allpollu"), reverse-engineered from the api.foobot.io cloud.

Why this exists
---------------
The Foobot publishes RAW integers over local MQTT (topic `.../sensor/push`).
The (now unreliable) Airboxlab cloud calibrated them and computed a global
index, `allpollu` (%), which drives the colour of the LED ring. This module
reproduces both steps LOCALLY so you never need the cloud again.

===============================================================================
1) DECODE raw -> calibrated   (validated against a raw/cloud pair, 2025 capture)
===============================================================================
  sensor/push raw : {"co2":2638,"voc":728,"temp":27511,"hum":56284,"pm":936,...}
  cloud, same instant : co2=2655 ppm, voc=732 ppb, tmp=27.48 C, hum=56.41 %,
                        pm=9.94 ugm3, allpollu=70.94 %
  => co2 x1 (ppm) ; voc x1 (ppb) ; temp /1000 (C) ; hum /1000 (%) ;
     pm : / ~94 (empirical factor, depends on humidity compensation pm_comp_trh).

===============================================================================
2) INDEX allpollu = f(pm, voc)   (regression on 135 cloud points, 7 days)
===============================================================================
  Key observations:
   - co2 ~= 3.62 x voc almost constantly => the Foobot DERIVES CO2 from VOC;
     there are really only 2 inputs: pm and voc.
   - pm alone explains only ~3 % of the variance; the ring reacts mostly to VOC.
   - Quadratic model (concave in voc = saturation): R^2 = 0.997, RMSE 0.96 %.

  allpollu ~= 0.9531*pm + 0.15775*voc - 7.216e-5*voc^2 - 17.697        (model B)
  (linear fallback, R^2=0.984 :  0.8096*pm + 0.10431*voc - 8.609)

===============================================================================
3) RING COLOUR  (from Foobot support docs)
===============================================================================
  allpollu < 50  => BLUE  (good)     : 3 sub-levels (great/good/fair)
  allpollu >= 50 => ORANGE then RED (bad) : 3 sub-levels
  Cited PM thresholds : 0-12 "great", > 37 "poor".

  NOTE: the ring colour the device *actually* shows is computed ON THE DEVICE
  from thresholds it receives on `attribute/thresholds` (cloud->device, never
  captured here). This module computes allpollu for your dashboard; driving the
  exact on-device colour remains an open point.

The calibration constants below were reversed from ONE specific unit. They are a
good starting point; if your unit reads slightly off, re-fit PM_DIVISOR and the
allpollu coefficients from your own raw/reference pairs.
"""

# --- 1) decode ----------------------------------------------------------------

PM_DIVISOR = 94.2   # empirical raw->ugm3 factor (raw 936 -> 9.94 ugm3)


def decode(raw: dict) -> dict:
    """Convert a dict of RAW readings (sensor/push) into calibrated units.
    Lenient: only returns the keys that are present."""
    out = {}
    if "co2" in raw:  out["co2"] = float(raw["co2"])              # ppm
    if "voc" in raw:  out["voc"] = float(raw["voc"])              # ppb
    if "temp" in raw: out["tmp"] = round(raw["temp"] / 1000, 2)   # C
    if "hum" in raw:  out["hum"] = round(raw["hum"] / 1000, 2)    # %
    if "pm" in raw:   out["pm"]  = round(raw["pm"] / PM_DIVISOR, 2)  # ugm3
    return out


# --- 2) allpollu index --------------------------------------------------------

# Model B (quadratic, R^2=0.997). Coefficients (pm, voc, voc^2, const).
_B = (0.9531, 0.15775, -7.216e-5, -17.697)


def allpollu(pm: float, voc: float) -> float:
    """Global pollution index (%) reproduced from the Foobot cloud.
    pm in ugm3, voc in ppb (CALIBRATED values)."""
    a, bv, bv2, c = _B
    val = a * pm + bv * voc + bv2 * voc * voc + c
    return max(0.0, round(val, 1))


# --- 3) colour / level --------------------------------------------------------

def band(ap: float) -> str:
    """Qualitative band from the allpollu index (%)."""
    if ap < 25:   return "great"    # deep blue
    if ap < 50:   return "good"     # blue
    if ap < 75:   return "fair"     # orange
    return "poor"                   # red


def color(ap: float) -> str:
    """Ring colour (blue if good, orange->red if bad)."""
    return "blue" if ap < 50 else ("orange" if ap < 75 else "red")


def enrich(raw: dict) -> dict:
    """Full pipeline: raw -> calibrated + allpollu + colour.
    Returns a dict ready to push to a dashboard / Home Assistant."""
    m = decode(raw)
    if "pm" in m and "voc" in m:
        ap = allpollu(m["pm"], m["voc"])
        m["allpollu"] = ap
        m["band"] = band(ap)
        m["color"] = color(ap)
    return m


if __name__ == "__main__":
    # Self-test on a real raw/cloud pair.
    raw = {"co2": 2638, "voc": 728, "temp": 27511, "hum": 56284, "pm": 936}
    cloud = {"pm": 9.94, "tmp": 27.48, "hum": 56.41, "co2": 2655, "voc": 732,
             "allpollu": 70.94}
    got = enrich(raw)
    print("raw     :", raw)
    print("decoded :", got)
    print("cloud   :", cloud)
    print(f"allpollu error = {got['allpollu'] - cloud['allpollu']:+.2f} pts "
          f"(local {got['allpollu']} vs cloud {cloud['allpollu']})")
