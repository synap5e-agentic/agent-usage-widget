import QtQuick
import QtQuick.Layouts
import QtQuick.Controls
import qs.Commons
import qs.Widgets

Item {
  id: root

  property var agents: []
  property var backend: ({})
  property string updatedAt: ""
  property real currentTime: Date.now()
  property bool showCloseButton: true
  property string title: "Agent Usage"
  property string emptyMessage: "Loading live usage snapshots from local service."
  property var accentColorFn: null

  signal closeRequested()

  readonly property real cardMinWidth: Math.round(330 * Style.uiScaleRatio)
  readonly property int cardColumns: orderedAgents.length >= 3 ? 3 : (orderedAgents.length > 1 ? 2 : 1)
  readonly property var orderedAgents: orderAgents(agents)

  readonly property var geometryPlaceholder: mainContainer
  property real contentPreferredWidth: Math.round((cardMinWidth * cardColumns) + (Style.marginL * (cardColumns + 3)))
  property real contentPreferredHeight: mainColumn.implicitHeight + Style.marginL * 2
  readonly property bool allowAttach: true

  anchors.fill: parent

  function accentColor(name) {
    if (accentColorFn) return accentColorFn(name || "primary");
    return Color.mPrimary;
  }

  function updatedLabel() {
    if (!updatedAt) return "Waiting for usage data";
    const d = new Date(updatedAt);
    if (isNaN(d.getTime())) return "Waiting for usage data";
    let h = d.getHours();
    const m = d.getMinutes();
    const suffix = h >= 12 ? "pm" : "am";
    h = h % 12;
    if (h === 0) h = 12;
    return "Updated " + h + ":" + (m < 10 ? "0" : "") + m + suffix;
  }

  function providerRank(agent) {
    const order = { "claude": 0, "codex": 1, "cursor": 2 };
    const id = (agent && agent.id) || "";
    return order.hasOwnProperty(id) ? order[id] : 99;
  }

  function orderAgents(list) {
    const items = (list || []).slice();
    items.sort(function(a, b) {
      const rankDiff = providerRank(a) - providerRank(b);
      if (rankDiff !== 0) return rankDiff;
      return (a.label || "").localeCompare(b.label || "");
    });
    return items;
  }

  function shouldShowMetric(agent, metric) {
    const provider = (agent && agent.id) || "";
    const key = ((metric && metric.metric_key) || "").toLowerCase();
    const providerKey = ((metric && metric.provider_metric_key) || "").toLowerCase();
    const path = ((metric && metric.metric_path) || "").toLowerCase();
    if (provider === "claude" && (key.indexOf("omelette") !== -1 || providerKey.indexOf("omelette") !== -1 || path.indexOf("omelette") !== -1))
      return false;
    if (provider === "cursor" && (key === "api_usage" || key === "auto_usage" || key === "provider_total_usage" || key === "included_spend" || key === "total_spend" || key.indexOf("model_") === 0))
      return false;
    return true;
  }

  function graphMetricIds(agent) {
    const ids = [];
    const graphs = (agent && agent.graphs) || {};
    for (const key of ["long_window", "short_window"]) {
      const graph = graphs[key];
      if (!graph) continue;
      if (graph.metric_path) ids.push(graph.metric_path);
      else if (graph.metric_key) ids.push(graph.metric_key);
    }
    return ids;
  }

  function primaryMetrics(agent) {
    const ids = graphMetricIds(agent);
    return ((agent && agent.metrics) || []).filter(function(metric) {
      const id = metric.metric_path || metric.metric_key || "";
      return ids.indexOf(id) !== -1 && shouldShowMetric(agent, metric);
    });
  }

  function secondaryMetrics(agent) {
    const ids = graphMetricIds(agent);
    return ((agent && agent.metrics) || []).filter(function(metric) {
      const id = metric.metric_path || metric.metric_key || "";
      return ids.indexOf(id) === -1 && shouldShowMetric(agent, metric);
    });
  }

  function statusMessage(agent) {
    const status = (agent && agent.status) || ({});
    const state = String(status.state || "ok");
    if (state === "ok") return "";
    return String(status.message || status.label || "").trim();
  }

  function graphAccent(metric) {
    const key = metric && metric.metric_key;
    if (key === "five_hour" || key === "primary_window" || key === "auto_spend") return "secondary";
    if (key === "secondary_window" || key === "spark_usage" || key === "spark") return "tertiary";
    if (key === "seven_day" || key === "sonnet_usage" || key === "sonnet") return "primary";
    return metric ? metric.accent : null;
  }

  Rectangle {
    id: mainContainer
    anchors.fill: parent
    color: Color.mSurfaceVariant

    ColumnLayout {
      id: mainColumn
      anchors.fill: parent
      anchors.margins: Style.marginL
      spacing: Style.marginM

      NBox {
        Layout.fillWidth: true
        implicitHeight: headerRow.implicitHeight + Style.marginXL

        RowLayout {
          id: headerRow
          anchors.fill: parent
          anchors.margins: Style.marginM
          spacing: Style.marginM

          NIcon {
            icon: "sparkles"
            pointSize: Style.fontSizeXXL
            color: Color.mPrimary
          }

          ColumnLayout {
            spacing: 2
            Layout.fillWidth: true

            NText {
              text: root.title
              pointSize: Style.fontSizeL
              font.weight: Style.fontWeightBold
              color: Color.mOnSurface
            }

            NText {
              text: (backend.label || "No backend") + "  |  " + updatedLabel()
              pointSize: Style.fontSizeXS
              color: Color.mOnSurfaceVariant
            }
          }

          NIconButton {
            visible: root.showCloseButton
            icon: "close"
            baseSize: Style.baseWidgetSize * 0.8
            onClicked: root.closeRequested()
          }
        }
      }

      NBox {
        Layout.fillWidth: true
        visible: !backend.kind
        implicitHeight: hintText.implicitHeight + Style.marginL

        NText {
          id: hintText
          anchors.fill: parent
          anchors.margins: Style.marginM
          wrapMode: Text.Wrap
          text: root.emptyMessage
          pointSize: Style.fontSizeXS
          color: Color.mOnSurfaceVariant
        }
      }

      GridLayout {
        id: cardsGrid
        Layout.fillWidth: true
        columns: root.cardColumns
        columnSpacing: Style.marginM
        rowSpacing: Style.marginM

        Repeater {
          model: orderedAgents

          delegate: NBox {
            required property var modelData

            Layout.fillWidth: true
            Layout.alignment: Qt.AlignTop
            Layout.preferredWidth: root.cardMinWidth
            implicitHeight: cardColumn.implicitHeight + Style.marginL

            ColumnLayout {
              id: cardColumn
              anchors.fill: parent
              anchors.margins: Style.marginM
              spacing: Style.marginM

              readonly property var primaryMetricRows: root.primaryMetrics(modelData)
              readonly property var secondaryMetricRows: root.secondaryMetrics(modelData)

              RowLayout {
                Layout.fillWidth: true

                ColumnLayout {
                  spacing: 2
                  Layout.fillWidth: true

                  NText {
                    text: modelData.label
                    pointSize: Style.fontSizeL
                    font.weight: Style.fontWeightBold
                    color: root.accentColor(modelData.accent)
                  }

                  NText {
                    text: modelData.summary ? modelData.summary.label + ": " + modelData.summary.value : "No summary"
                    pointSize: Style.fontSizeS
                    color: Color.mOnSurface
                    Layout.fillWidth: true
                  }

                  NText {
                    visible: modelData.summary && !!modelData.summary.note
                    text: modelData.summary ? modelData.summary.note : ""
                    pointSize: Style.fontSizeXS
                    color: Color.mOnSurfaceVariant
                    Layout.fillWidth: true
                    wrapMode: Text.Wrap
                  }

                  NText {
                    visible: root.statusMessage(modelData).length > 0
                    text: root.statusMessage(modelData)
                    pointSize: Style.fontSizeXS
                    color: root.accentColor(modelData.accent || "primary")
                    Layout.fillWidth: true
                    wrapMode: Text.Wrap
                  }
                }
              }

              ColumnLayout {
                Layout.fillWidth: true
                spacing: Style.marginS

                Repeater {
                  model: cardColumn.primaryMetricRows

                  delegate: ColumnLayout {
                    required property var modelData
                    Layout.fillWidth: true
                    spacing: Style.marginXS

                    RowLayout {
                      Layout.fillWidth: true

                      NText {
                        text: modelData.label
                        pointSize: Style.fontSizeS
                        color: Color.mOnSurface
                        Layout.fillWidth: true
                      }

                      NText {
                        text: modelData.value
                        pointSize: Style.fontSizeS
                        font.family: Settings.data.ui.fontFixed
                        font.weight: Style.fontWeightBold
                        color: modelData.percent !== undefined ? root.accentColor(modelData.accent || "primary") : Color.mOnSurface
                      }
                    }

                    Rectangle {
                      visible: modelData.show_bar !== false && modelData.percent !== undefined
                      Layout.fillWidth: true
                      height: 4 * Style.uiScaleRatio
                      radius: 2
                      color: Color.mSurfaceVariant

                      Rectangle {
                        width: parent.width * (Math.max(0, Math.min(100, modelData.percent)) / 100)
                        height: parent.height
                        radius: parent.radius
                        color: root.accentColor(modelData.accent || "primary")
                        Behavior on width { NumberAnimation { duration: 300 } }
                      }
                    }
                  }
                }
              }

              ColumnLayout {
                Layout.fillWidth: true
                spacing: Style.marginXS

                Repeater {
                  model: [
                    modelData.graphs ? modelData.graphs.long_window : null,
                    modelData.graphs ? modelData.graphs.short_window : null
                  ]

                  delegate: ColumnLayout {
                    required property var modelData
                    visible: modelData && modelData.points && modelData.points.length > 0
                    Layout.fillWidth: true
                    spacing: Style.marginXS

                    NText {
                      text: modelData ? modelData.label : "Usage window"
                      pointSize: Style.fontSizeXS
                      color: Color.mOnSurfaceVariant
                    }

                    UsageGraph {
                      Layout.fillWidth: true
                      Layout.preferredHeight: Math.round(120 * Style.uiScaleRatio)
                      graph: modelData || ({})
                      nowMs: root.currentTime
                      accentColor: root.accentColor(root.graphAccent(modelData))
                    }
                  }
                }
              }

              ColumnLayout {
                Layout.fillWidth: true
                spacing: Style.marginS
                visible: cardColumn.secondaryMetricRows.length > 0

                Repeater {
                  model: cardColumn.secondaryMetricRows

                  delegate: ColumnLayout {
                    required property var modelData
                    Layout.fillWidth: true
                    spacing: Style.marginXS

                    RowLayout {
                      Layout.fillWidth: true

                      NText {
                        text: modelData.label
                        pointSize: Style.fontSizeS
                        color: Color.mOnSurfaceVariant
                        Layout.fillWidth: true
                      }

                      NText {
                        text: modelData.value
                        pointSize: Style.fontSizeS
                        font.family: Settings.data.ui.fontFixed
                        color: Color.mOnSurfaceVariant
                      }
                    }

                    Rectangle {
                      visible: modelData.show_bar !== false && modelData.percent !== undefined
                      Layout.fillWidth: true
                      height: 4 * Style.uiScaleRatio
                      radius: 2
                      color: Qt.rgba(Color.mOnSurfaceVariant.r, Color.mOnSurfaceVariant.g, Color.mOnSurfaceVariant.b, 0.15)

                      Rectangle {
                        width: parent.width * (Math.max(0, Math.min(100, modelData.percent)) / 100)
                        height: parent.height
                        radius: parent.radius
                        color: Qt.rgba(Color.mOnSurfaceVariant.r, Color.mOnSurfaceVariant.g, Color.mOnSurfaceVariant.b, 0.55)
                        Behavior on width { NumberAnimation { duration: 300 } }
                      }
                    }
                  }
                }
              }

              ColumnLayout {
                Layout.fillWidth: true
                spacing: Style.marginXS
                visible: (modelData.details || []).length > 0

                Repeater {
                  model: modelData.details || []

                  delegate: RowLayout {
                    required property var modelData
                    Layout.fillWidth: true

                    NText {
                      text: modelData.label
                      pointSize: Style.fontSizeXS
                      color: Color.mOnSurfaceVariant
                      Layout.preferredWidth: Math.round(86 * Style.uiScaleRatio)
                    }

                    NText {
                      text: modelData.value
                      pointSize: Style.fontSizeXS
                      color: Color.mOnSurface
                      Layout.fillWidth: true
                      wrapMode: Text.Wrap
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
