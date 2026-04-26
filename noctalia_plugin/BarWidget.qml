import QtQuick
import Quickshell
import qs.Commons
import qs.Services.UI
import "qml" as AgentUsageUi

Item {
  id: root

  property var pluginApi: null
  property ShellScreen screen
  property string widgetId: ""
  property string section: ""
  property int sectionWidgetIndex: -1
  property int sectionWidgetsCount: 0

  readonly property string screenName: screen ? screen.name : ""
  readonly property string barPosition: Settings.getBarPositionForScreen(screenName)
  readonly property bool isVertical: barPosition === "left" || barPosition === "right"
  readonly property real capsuleHeight: Style.getCapsuleHeightForScreen(screenName)
  readonly property real barFontSize: Style.getBarFontSizeForScreen(screenName)
  readonly property real iconSize: Style.toOdd(capsuleHeight * 0.48)

  readonly property var mainInstance: pluginApi?.mainInstance
  readonly property var agents: mainInstance ? (mainInstance.agents || []) : []
  readonly property bool hasData: agents.length > 0

  implicitWidth: widget.implicitWidth
  implicitHeight: widget.implicitHeight

  function statusMessage(agent) {
    const status = (agent && agent.status) || ({});
    const state = String(status.state || "ok");
    if (state === "ok") return "";
    return String(status.message || status.label || "").trim();
  }

  AgentUsageUi.AgentUsageBar {
    id: widget
    anchors.fill: parent
    agents: root.agents
    isVertical: root.isVertical
    capsuleHeight: root.capsuleHeight
    barFontSize: root.barFontSize
    iconSize: root.iconSize
    accentColorFn: function(name) {
      return root.mainInstance ? root.mainInstance.accentColor(name || "primary") : "#ff7a1a";
    }
  }

  MouseArea {
    anchors.fill: parent
    hoverEnabled: true
    acceptedButtons: Qt.LeftButton

    onClicked: {
      if (root.pluginApi) root.pluginApi.togglePanel(root.screen, root);
    }

    onEntered: {
      if (!root.hasData) return;
      let summary = root.mainInstance ? root.mainInstance.backendSummary() : "Agent usage";
      for (let i = 0; i < root.agents.length; i++) {
        const agent = root.agents[i];
        summary += "\n" + agent.label + ": " + (agent.summary ? agent.summary.value : "--");
        if (agent.summary && agent.summary.note) summary += " (" + agent.summary.note + ")";
        const statusText = root.statusMessage(agent);
        if (statusText) summary += " [" + statusText + "]";
      }
      TooltipService.show(root, summary, BarService.getTooltipDirection());
    }

    onExited: {
      TooltipService.hide();
    }
  }
}
