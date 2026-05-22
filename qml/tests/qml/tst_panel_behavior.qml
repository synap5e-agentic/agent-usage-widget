import QtQuick
import QtTest

TestCase {
  id: testRoot
  name: "PanelBehavior"

  function createPanel() {
    const component = Qt.createComponent(Qt.resolvedUrl("../../AgentUsagePanel.qml"));
    compare(component.status, Component.Ready, component.errorString());
    const panel = component.createObject(testRoot, { width: 900, height: 500 });
    verify(panel !== null);
    return panel;
  }

  function test_orders_agents_by_frontend_policy() {
    const panel = createPanel();

    const ordered = panel.orderAgents([
      { id: "work", label: "Cursor", frontend: { order: 30 } },
      { id: "other", label: "Other" },
      { id: "personal", label: "Claude", frontend: { order: 10 } },
      { id: "codex", label: "Codex", frontend: { order: 20 } },
    ]);

    compare(ordered.map(function(agent) { return agent.id; }).join(","), "personal,codex,work,other");
    panel.destroy();
  }

  function test_panel_background_is_not_transparent() {
    const panel = createPanel();
    verify(panel.geometryPlaceholder !== null);
    verify(panel.geometryPlaceholder.color.a > 0.5);
    panel.destroy();
  }

  function test_status_message_only_shows_non_ok_status() {
    const panel = createPanel();

    compare(panel.statusMessage({ status: { state: "ok", message: "ignored" } }), "");
    compare(
      panel.statusMessage({ status: { state: "error", message: "Sign-in expired. Showing data from 10:39am." } }),
      "Sign-in expired. Showing data from 10:39am."
    );

    panel.destroy();
  }

  function test_metric_sections_follow_frontend_policy() {
    const panel = createPanel();
    const cursor = {
      id: "cursor",
      frontend: { graph_order: ["long_window", "short_window"] },
      graphs: {
        long_window: { metric_path: "/monthly" },
        short_window: { metric_path: "/auto" },
      },
      metrics: [
        { metric_key: "monthly", metric_path: "/monthly", label: "Monthly", frontend: { visible: true, section: "primary" } },
        { metric_key: "auto_spend", metric_path: "/auto", label: "Auto", frontend: { visible: true, section: "primary" } },
        { metric_key: "api_usage", metric_path: "/api", label: "API", frontend: { visible: false, section: "secondary" } },
        { metric_key: "over_cap_used", metric_path: "/over", label: "Over cap", frontend: { visible: true, section: "secondary" } },
      ],
    };

    const primary = panel.primaryMetrics(cursor);
    const secondary = panel.secondaryMetrics(cursor);

    compare(primary.length, 2);
    compare(primary[0].metric_key, "monthly");
    compare(primary[1].metric_key, "auto_spend");
    compare(secondary.length, 1);
    compare(secondary[0].metric_key, "over_cap_used");
    verify(!panel.shouldShowMetric(cursor, cursor.metrics[2]));

    panel.destroy();
  }
}
