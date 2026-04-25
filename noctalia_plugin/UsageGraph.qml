import QtQuick
import qs.Commons

Canvas {
  id: root

  property var graph: ({})
  property color accentColor: Color.mPrimary
  property real nowMs: Date.now()

  readonly property real padLeft: 28 * Style.uiScaleRatio
  readonly property real padRight: 10 * Style.uiScaleRatio
  readonly property real padTop: 10 * Style.uiScaleRatio
  readonly property real padBottom: 22 * Style.uiScaleRatio
  readonly property real plotW: width - padLeft - padRight
  readonly property real plotH: height - padTop - padBottom

  property int _pointCount: graph && graph.points ? graph.points.length : 0

  onGraphChanged: requestPaint()
  on_PointCountChanged: requestPaint()
  onNowMsChanged: requestPaint()

  function formatEdgeLabel(timestampMs, spanMs) {
    const d = new Date(timestampMs);
    if (spanMs <= 36 * 60 * 60 * 1000) {
      let h = d.getHours();
      const m = d.getMinutes();
      const suffix = h >= 12 ? "pm" : "am";
      h = h % 12;
      if (h === 0) h = 12;
      return m === 0 ? (h + suffix) : (h + ":" + (m < 10 ? "0" : "") + m + suffix);
    }
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return months[d.getMonth()] + " " + d.getDate();
  }

  function formatYAxisLabel(value, maxValue, valueKind) {
    if (valueKind === "currency_cents") {
      const dollars = value / 100.0;
      if (maxValue >= 10000) return "$" + Math.round(dollars);
      if (maxValue >= 1000) return "$" + dollars.toFixed(1);
      return "$" + dollars.toFixed(2);
    }
    return Math.round(value) + "%";
  }

  function preparedGraphPoints(rawPoints, minTs, maxTs, hasWindowBounds) {
    const byTimestamp = ({});
    for (let i = 0; i < rawPoints.length; i++) {
      const point = rawPoints[i] || {};
      const t = Number(point.t || 0);
      const value = Number(point.value || 0);
      if (!isFinite(t) || !isFinite(value) || t <= 0) continue;
      const tMs = t * 1000;
      if (hasWindowBounds && (tMs < minTs || tMs > maxTs)) continue;
      const key = String(Math.round(t));
      if (!byTimestamp[key] || value > byTimestamp[key].value) {
        byTimestamp[key] = { t: Math.round(t), value: value };
      }
    }

    const points = [];
    for (const key in byTimestamp) points.push(byTimestamp[key]);
    points.sort(function(a, b) { return a.t - b.t; });
    return points;
  }

  function graphSegments(points) {
    const segments = [];
    let current = [];
    for (let i = 0; i < points.length; i++) {
      const point = points[i];
      if (current.length > 0) {
        const previous = current[current.length - 1];
        if (point.t <= previous.t || point.value < previous.value) {
          segments.push(current);
          current = [];
        }
      }
      current.push(point);
    }
    if (current.length > 0) segments.push(current);
    return segments;
  }

  function graphCoords(points, minTs, span, maxValue) {
    const coords = [];
    for (let i = 0; i < points.length; i++) {
      const frac = (points[i].t * 1000 - minTs) / span;
      const x = padLeft + Math.max(0, Math.min(1, frac)) * plotW;
      const y = padTop + plotH * (1 - Math.max(0, Math.min(maxValue, points[i].value)) / maxValue);
      coords.push({ x: x, y: y });
    }
    return coords;
  }

  onPaint: {
    const ctx = getContext("2d");
    ctx.clearRect(0, 0, width, height);
    if (!graph || !graph.points || graph.points.length === 0) return;

    const pL = padLeft;
    const pT = padTop;
    const pW = plotW;
    const pH = plotH;
    if (pW <= 0 || pH <= 0) return;

    const rawPoints = graph.points;
    const valueKind = graph.value_kind || "percent";
    const maxValue = Math.max(1, Number(graph.max_value || (valueKind === "currency_cents" ? 100 : 100)));
    const showPaceLine = graph.pace_line !== false;
    const windowStart = graph.window_start ? new Date(graph.window_start).getTime() : NaN;
    const windowEnd = graph.window_end ? new Date(graph.window_end).getTime() : NaN;
    const hasWindowBounds = !isNaN(windowStart) && !isNaN(windowEnd) && windowEnd > windowStart;
    const minTs = hasWindowBounds ? windowStart : rawPoints[0].t * 1000;
    const maxTs = hasWindowBounds ? windowEnd : rawPoints[rawPoints.length - 1].t * 1000;
    const points = preparedGraphPoints(rawPoints, minTs, maxTs, hasWindowBounds);
    const span = Math.max(1, maxTs - minTs);

    ctx.font = Math.round(9 * Style.uiScaleRatio) + "px " + (Settings.data.ui.fontFixed || "monospace");
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";

    const tickFractions = [0, 0.25, 0.5, 0.75, 1];
    for (let i = 0; i < tickFractions.length; i++) {
      const tickValue = maxValue * tickFractions[i];
      const y = pT + pH * (1 - tickFractions[i]);
      ctx.strokeStyle = Qt.rgba(Color.mOutline.r, Color.mOutline.g, Color.mOutline.b, 0.15);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pL, y);
      ctx.lineTo(pL + pW, y);
      ctx.stroke();
      ctx.fillStyle = Color.mOnSurfaceVariant;
      ctx.fillText(formatYAxisLabel(tickValue, maxValue, valueKind), pL - 4, y);
    }

    ctx.textBaseline = "top";
    ctx.fillStyle = Color.mOnSurfaceVariant;
    ctx.textAlign = "left";
    ctx.fillText(formatEdgeLabel(minTs, span), pL, pT + pH + 6);
    ctx.textAlign = "right";
    ctx.fillText(formatEdgeLabel(maxTs, span), pL + pW, pT + pH + 6);

    if (windowEnd > windowStart && showPaceLine) {
      ctx.strokeStyle = Qt.rgba(Color.mOutline.r, Color.mOutline.g, Color.mOutline.b, 0.65);
      ctx.setLineDash([4, 3]);
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(pL, pT + pH);
      ctx.lineTo(pL + pW, pT);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    const liveTs = Math.max(minTs, Math.min(maxTs, nowMs || Date.now()));
    if (liveTs >= minTs && liveTs <= maxTs) {
      const nowFrac = (liveTs - minTs) / span;
      const nowX = pL + Math.max(0, Math.min(1, nowFrac)) * pW;
      ctx.strokeStyle = Qt.rgba(Color.mOnSurfaceVariant.r, Color.mOnSurfaceVariant.g, Color.mOnSurfaceVariant.b, 0.7);
      ctx.setLineDash([3, 3]);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(nowX, pT);
      ctx.lineTo(nowX, pT + pH);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    const segments = graphSegments(points);
    if (segments.length === 0) return;
    const hasLeadingGap = (points[0].t * 1000) > (minTs + Math.max(60 * 1000, span * 0.01));

    if (!hasLeadingGap) {
      const grad = ctx.createLinearGradient(0, pT, 0, pT + pH);
      grad.addColorStop(0, Qt.rgba(accentColor.r, accentColor.g, accentColor.b, 0.20));
      grad.addColorStop(1, "transparent");
      ctx.fillStyle = grad;
      for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
        const coords = graphCoords(segments[segmentIndex], minTs, span, maxValue);
        ctx.beginPath();
        ctx.moveTo(coords[0].x, pT + pH);
        for (let i = 0; i < coords.length; i++) ctx.lineTo(coords[i].x, coords[i].y);
        ctx.lineTo(coords[coords.length - 1].x, pT + pH);
        ctx.closePath();
        ctx.fill();
      }
    }

    ctx.strokeStyle = accentColor;
    ctx.lineWidth = 2 * Style.uiScaleRatio;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    for (let segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
      const coords = graphCoords(segments[segmentIndex], minTs, span, maxValue);
      ctx.beginPath();
      ctx.moveTo(coords[0].x, coords[0].y);
      for (let i = 1; i < coords.length; i++) ctx.lineTo(coords[i].x, coords[i].y);
      ctx.stroke();
    }

    const lastSegment = segments[segments.length - 1];
    const lastCoords = graphCoords(lastSegment, minTs, span, maxValue);
    const last = lastCoords[lastCoords.length - 1];
    const lastTsMs = points[points.length - 1].t * 1000;
    const guideEndTs = Math.max(lastTsMs, Math.min(maxTs, nowMs || Date.now()));
    if (guideEndTs > lastTsMs) {
      const guideFrac = (guideEndTs - minTs) / span;
      const guideX = pL + Math.max(0, Math.min(1, guideFrac)) * pW;
      ctx.strokeStyle = Qt.rgba(accentColor.r, accentColor.g, accentColor.b, 0.4);
      ctx.setLineDash([3, 3]);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(last.x, last.y);
      ctx.lineTo(guideX, last.y);
      ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.fillStyle = accentColor;
    ctx.beginPath();
    ctx.arc(last.x, last.y, 3 * Style.uiScaleRatio, 0, Math.PI * 2);
    ctx.fill();
  }
}
