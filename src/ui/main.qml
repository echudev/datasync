// Importación de módulos necesarios de Qt
import QtQuick 2.15
import QtQuick.Controls 2.15
import QtQuick.Layouts 1.15
import QtGraphicalEffects 1.15
import Theme 1.0

// Ventana principal de la aplicación
ApplicationWindow {
    onClosing: {
        close.accepted = false; // Prevenir cierre
        if (python && python.minimizeToTray) {
            python.minimizeToTray();
        }
    }
    property int navIndex: 0 // Default to Services view
    visible: true
    width: 700
    height: 500
    title: "DataSync Datalogger"

    // Fondo de la ventana
    Rectangle {
        id: backgroundRect
        anchors.fill: parent
        color: Theme.primary
    }

    // Layout principal: contiene el menú de navegación y el área de vistas
    ColumnLayout {
        anchors.fill: parent
        spacing: 10

        // Fondo del navbar
        Rectangle {
            id: navbarRect
            color: Qt.rgba(255, 255, 255, 0.1)
            border.color: Qt.rgba(255, 255, 255, 0.5)
            border.width: 1
            radius: 8
            Layout.fillWidth: true
            Layout.preferredHeight: 60
            z: 100

            // Highlight animado fuera del RowLayout
            Rectangle {
                id: highlight
                color: "#FFD700"
                radius: 8
                width: navMenu.buttonRects[navIndex].width
                height: navMenu.buttonRects[navIndex].height
                x: navMenu.buttonRects[navIndex].mapToItem(navbarRect, 0, 0).x
                y: navMenu.buttonRects[navIndex].mapToItem(navbarRect, 0, 0).y
                z: -1
                Behavior on x { NumberAnimation { duration: 200; easing.type: Easing.InOutQuad } }
                Behavior on width { NumberAnimation { duration: 200; easing.type: Easing.InOutQuad } }
            }

            RowLayout {
                id: navMenu
                property var buttonRects: [botonEquipos, botonCronjobs, botonMediciones, botonLogs]
                Layout.fillWidth: true
                Layout.fillHeight: true
                Layout.alignment: Qt.AlignLeft
                spacing: 0

                // Botón para ir a Equipos
                Rectangle {
                    id: botonEquipos
                    width: 120
                    height: 60
                    color: "transparent"
                    radius: 8
                    border.color: "transparent"
                    MouseArea {
                        anchors.fill: parent
                        onClicked: navIndex = 0
                        cursorShape: Qt.PointingHandCursor
                    }
                    Text {
                        text: "Equipos"
                        anchors.centerIn: parent
                        color: navIndex === 0 ? "#222C50" : "white"
                        font.bold: navIndex === 0
                    }
                }
                // Botón para ir a Tareas Programadas (CronJobs)
                Rectangle {
                    id: botonCronjobs
                    width: 120
                    height: 60
                    color: "transparent"
                    radius: 8
                    border.color: "transparent"
                    MouseArea {
                        anchors.fill: parent
                        onClicked: navIndex = 1
                        cursorShape: Qt.PointingHandCursor
                    }
                    Text {
                        text: "CronJobs"
                        anchors.centerIn: parent
                        color: navIndex === 1 ? "#222C50" : "white"
                        font.bold: navIndex === 1
                    }
                }
                // Botón para ir a Mediciones
                Rectangle {
                    id: botonMediciones
                    width: 120
                    height: 60
                    color: "transparent"
                    radius: 8
                    border.color: "transparent"
                    MouseArea {
                        anchors.fill: parent
                        onClicked: navIndex = 2
                        cursorShape: Qt.PointingHandCursor
                    }
                    Text {
                        text: "Mediciones"
                        anchors.centerIn: parent
                        color: navIndex === 2 ? "#222C50" : "white"
                        font.bold: navIndex === 2
                    }
                }
                // Botón para ir a Tareas
                Rectangle {
                    id: botonLogs
                    width: 120
                    height: 60
                    color: "transparent"
                    radius: 8
                    border.color: "transparent"
                    MouseArea {
                        anchors.fill: parent
                        onClicked: navIndex = 3
                        cursorShape: Qt.PointingHandCursor
                    }
                    Text {
                        text: "Logbook"
                        anchors.centerIn: parent
                        color: navIndex === 3 ? "#222C50" : "white"
                        font.bold: navIndex === 3
                    }
                }
            }
        }

        // Área principal donde se muestran las vistas según la navegación
        StackLayout {
            id: mainStack
            Layout.fillWidth: true
            Layout.fillHeight: true
            currentIndex: navIndex

            // Loader para la vista de Equipos
            Loader {
                source: "views/DevicesView.qml"
            }
             // Loader para la vista de Cronjobs
            Loader {
                source: "views/CronJobsView.qml"
            }
            // Loader para la vista de Mediciones
            Loader {
                source: "views/MeasuresView.qml"
            }
            // Loader para la vista de Logs
            Loader {
                source: "views/LogsView.qml"
            }
        }
    }
}
