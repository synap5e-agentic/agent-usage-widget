import QtQuick
import "qml" as AgentUsageUi

Item {
  id: root

  property var pluginApi: null

  readonly property var mainInstance: pluginApi?.mainInstance

  readonly property var geometryPlaceholder: panel.geometryPlaceholder
  property real contentPreferredWidth: panel.contentPreferredWidth
  property real contentPreferredHeight: panel.contentPreferredHeight
  readonly property bool allowAttach: panel.allowAttach

  anchors.fill: parent

  AgentUsageUi.AgentUsagePanel {
    id: panel
    anchors.fill: parent
    agents: root.mainInstance ? (root.mainInstance.agents || []) : []
    backend: root.mainInstance ? (root.mainInstance.backend || ({})) : ({})
    updatedAt: root.mainInstance ? root.mainInstance.updatedAt : ""
    currentTime: root.mainInstance ? root.mainInstance.currentTime : Date.now()
    accentColorFn: function(name) {
      return root.mainInstance ? root.mainInstance.accentColor(name || "primary") : "#ff7a1a";
    }
    onCloseRequested: {
      if (root.pluginApi) {
        root.pluginApi.withCurrentScreen(function(screen) { root.pluginApi.closePanel(screen); });
      }
    }
  }
}
