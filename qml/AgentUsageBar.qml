import QtQuick
import QtQuick.Layouts
import qs.Commons
import qs.Widgets

Item {
  id: root

  property var agents: []
  property bool isVertical: false
  property real capsuleHeight: Style.baseWidgetSize
  property real barFontSize: Style.fontSizeS
  property real iconSize: Style.toOdd(capsuleHeight * 0.48)
  property var accentColorFn: null

  function accentColor(name) {
    if (accentColorFn) return accentColorFn(name || "primary");
    return Color.mPrimary;
  }

  implicitWidth: isVertical ? capsuleHeight : mainLayout.implicitWidth + Style.marginXL
  implicitHeight: capsuleHeight

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
            color: root.accentColor(modelData.accent || "primary")
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
              color: root.accentColor(modelData.accent || "primary")
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
}
