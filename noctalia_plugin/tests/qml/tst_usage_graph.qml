import QtQuick
import QtTest

TestCase {
  id: testRoot
  name: "UsageGraph"

  function createGraph(properties) {
    const component = Qt.createComponent(Qt.resolvedUrl("../../UsageGraph.qml"));
    compare(component.status, Component.Ready, component.errorString());
    const graph = component.createObject(testRoot, Object.assign({
      width: 360,
      height: 180,
      nowMs: Date.parse("2026-04-01T03:00:00Z"),
    }, properties || {}));
    verify(graph !== null);
    return graph;
  }

  function epoch(raw) {
    return Date.parse(raw) / 1000;
  }

  function test_axis_labels() {
    const graph = createGraph();

    compare(graph.formatEdgeLabel(new Date(2026, 3, 1, 13, 30).getTime(), 5 * 60 * 60 * 1000), "1:30pm");
    compare(graph.formatEdgeLabel(new Date(2026, 3, 3, 0, 0).getTime(), 7 * 24 * 60 * 60 * 1000), "Apr 3");
    compare(graph.formatYAxisLabel(50, 100, "percent"), "50%");
    compare(graph.formatYAxisLabel(525, 2000, "currency_cents"), "$5.3");
    compare(graph.formatYAxisLabel(12500, 15000, "currency_cents"), "$125");

    graph.destroy();
  }

  function test_graph_change_updates_point_count() {
    const graph = createGraph({
      graph: {
        window_start: "2026-04-01T00:00:00Z",
        window_end: "2026-04-01T06:00:00Z",
        points: [
          { t: Date.parse("2026-04-01T01:00:00Z") / 1000, value: 5 },
          { t: Date.parse("2026-04-01T02:00:00Z") / 1000, value: 15 },
        ],
      },
    });

    compare(graph._pointCount, 2);
    graph.graph = { points: [] };
    compare(graph._pointCount, 0);

    graph.destroy();
  }

  function test_prepared_points_sort_filter_and_dedupe() {
    const graph = createGraph();
    const minTs = Date.parse("2026-04-01T00:00:00Z");
    const maxTs = Date.parse("2026-04-01T06:00:00Z");

    const points = graph.preparedGraphPoints([
      { t: epoch("2026-04-01T03:00:00Z"), value: 30 },
      { t: epoch("2026-03-31T23:59:00Z"), value: 99 },
      { t: epoch("2026-04-01T01:00:00Z"), value: 10 },
      { t: epoch("2026-04-01T03:00:00Z"), value: 34 },
      { t: epoch("2026-04-01T03:00:00Z"), value: 31 },
      { t: epoch("2026-04-01T07:00:00Z"), value: 80 },
    ], minTs, maxTs, true);

    compare(points.length, 2);
    compare(points[0].t, epoch("2026-04-01T01:00:00Z"));
    compare(points[0].value, 10);
    compare(points[1].t, epoch("2026-04-01T03:00:00Z"));
    compare(points[1].value, 34);

    graph.destroy();
  }

  function test_graph_segments_break_on_reset_drop() {
    const graph = createGraph();

    const segments = graph.graphSegments([
      { t: epoch("2026-04-01T01:00:00Z"), value: 72 },
      { t: epoch("2026-04-01T02:00:00Z"), value: 88 },
      { t: epoch("2026-04-01T03:00:00Z"), value: 3 },
      { t: epoch("2026-04-01T04:00:00Z"), value: 9 },
    ]);

    compare(segments.length, 2);
    compare(segments[0].length, 2);
    compare(segments[1].length, 2);
    compare(segments[0][1].value, 88);
    compare(segments[1][0].value, 3);

    graph.destroy();
  }

  function test_pace_line_targets_reference_value_for_over_cap_percent_graphs() {
    const graph = createGraph();

    compare(graph.paceLineTargetValue("percent", 200, 100), 100);
    compare(graph.paceLineTargetValue("percent", 100, 100), 100);
    compare(graph.paceLineTargetValue("percent", 150, 0), 150);
    compare(graph.paceLineTargetValue("currency_cents", 5000, 100), 5000);

    graph.destroy();
  }

  function test_empty_graph_can_paint_without_throwing() {
    const graph = createGraph({ graph: { points: [] } });

    graph.requestPaint();
    wait(50);
    compare(graph._pointCount, 0);

    graph.destroy();
  }
}
