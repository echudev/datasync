pragma Singleton
import QtQuick 2.0

QtObject {
    property color primary: Qt.rgba(25/255, 118/255, 210/255, 1.0)
    property color secondary: Qt.rgba(66/255, 66/255, 66/255, 1.0)      
    property color background: Qt.rgba(112/255, 14/255, 14/255, 0.45)
    property color accent:Qt.rgba(7/255, 255/255, 36/255, 0.45)
    property color deviceRunningBkg:Qt.rgba(0/255, 255/255, 0/255, 0.6)  //rgba(0, 255, 0, 0.6)
    property color deviceRunningBorder:Qt.rgba(0/255, 255/255, 0/255, 1.0)  //rgba(0, 255, 0, 1)
    property color deviceStoppedBkg:Qt.rgba(255/255, 0/255, 0/255, 0.6)  //rgba(255, 0, 0, 0.6)
    property color deviceStoppedBorder:Qt.rgba(255/255, 0/255, 0/255, 1.0)  //rgb(255, 0, 0)
}
