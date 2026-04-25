import QtQuick
import QtQuick.Layouts
import Quickshell
import qs.Commons
import qs.Widgets
import qs.Services.UI

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

  implicitWidth: isVertical ? capsuleHeight : mainLayout.implicitWidth + Style.marginXL
  implicitHeight: capsuleHeight

  function statusMessage(agent) {
    const status = (agent && agent.status) || ({});
    const state = String(status.state || "ok");
    if (state === "ok") return "";
    return String(status.message || status.label || "").trim();
  }

  Rectangle {
    id: capsule
    x: Style.pixelAlignCenter(parent.width, width)
    y: Style.pixelAlignCenter(parent.height, height)
    width: parent.width
    height: parent.height
    color: Style.capsuleColor
    radius: Style.radiusL
    border.color: Style.capsuleBorderColor
    border.width: Style.capsuleBorderWidth

    RowLayout {
      id: mainLayout
      anchors.centerIn: parent
      spacing: Style.marginS

      NIcon {
        icon: "sparkles"
        applyUiScale: false
        pointSize: root.barFontSize
        color: Color.mPrimary
      }

      Repeater {
        model: root.agents

        delegate: RowLayout {
          required property var modelData
          spacing: Style.marginXS
          visible: !root.isVertical

          NText {
            text: modelData.short_label || modelData.label
            font.family: Settings.data.ui.fontFixed
            font.weight: Style.fontWeightBold
            pointSize: root.barFontSize
            color: root.mainInstance ? root.mainInstance.accentColor(modelData.accent || "primary") : Color.mPrimary
          }

          Rectangle {
            width: Math.max(3, Style.toOdd(root.iconSize * 0.25))
            height: root.iconSize
            radius: width / 2
            color: Color.mOutline
            Layout.alignment: Qt.AlignVCenter

            Rectangle {
              property real fillHeight: parent.height * Math.min(1, Math.max(0, (modelData.summary && modelData.summary.percent ? modelData.summary.percent : 0) / 100))
              width: parent.width
              height: fillHeight
              radius: parent.radius
              color: root.mainInstance ? root.mainInstance.accentColor(modelData.accent || "primary") : Color.mPrimary
              anchors.bottom: parent.bottom

              Behavior on fillHeight {
                enabled: !Settings.data.general.animationDisabled
                NumberAnimation { duration: Style.animationNormal; easing.type: Easing.OutCubic }
              }
            }
          }

          NText {
            text: modelData.summary ? modelData.summary.value : "--"
            font.family: Settings.data.ui.fontFixed
            pointSize: root.barFontSize
            color: Color.mOnSurface
          }
        }
      }
    }
  }

  MouseArea {
    anchors.fill: parent
    hoverEnabled: true
    acceptedButtons: Qt.LeftButton

    onClicked: {
      if (pluginApi) pluginApi.togglePanel(screen, root);
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
