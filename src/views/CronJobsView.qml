// Importación de módulos necesarios de Qt
import QtQuick 2.15
import QtQuick.Controls 2.15
import QtQuick.Layouts 1.15

// Vista de Servicios: fondo con etiqueta centrada
ColumnLayout {
    Layout.fillWidth: true
    Layout.fillHeight: true
    spacing: 20
    
    // Propiedades para mantener el estado de los cronjobs
    property string csvPublisherState: python ? python.get_service_state("csv_publisher") : "STOPPED"
    property string winaqmsPublisherState: python ? python.get_service_state("winaqms_publisher") : "STOPPED"
    
    Rectangle {
        Layout.fillWidth: true
        height: 100
        color: csvPublisherState === "RUNNING" ? "#e6ffe6" : "#ffe6e6"
        radius: 5
        
        RowLayout {
            anchors.fill: parent
            anchors.margins: 10
            spacing: 10
            ColumnLayout {
                Layout.fillWidth: true
                Label {
                    text: "CSV Publisher"
                    font.bold: true
                }
                Label {
                    text: "Publica datos en formato CSV"
                    font.pixelSize: 12
                }
            }
            Button {
                text: "INICIAR"
                enabled: csvPublisherState === "STOPPED"
                onClicked: if (python) python.start_task("csv_publisher")
            }
            Button {
                text: "DETENER"
                enabled: csvPublisherState === "RUNNING"
                onClicked: if (python) python.stop_task("csv_publisher")
            }
        }
    }

    Rectangle {
        Layout.fillWidth: true
        height: 100
        color: winaqmsPublisherState === "RUNNING" ? "#e6ffe6" : "#ffe6e6"
        radius: 5
        
        RowLayout {
            anchors.fill: parent
            anchors.margins: 10
            spacing: 10
            ColumnLayout {
                Layout.fillWidth: true
                Label {
                    text: "WinAQMS Publisher"
                    font.bold: true
                }
                Label {
                    text: "Publica datos en formato WinAQMS"
                    font.pixelSize: 12
                }
            }
            Button {
                text: "INICIAR"
                enabled: winaqmsPublisherState === "STOPPED"
                onClicked: if (python) python.start_task("winaqms_publisher")
            }
            Button {
                text: "DETENER"
                enabled: winaqmsPublisherState === "RUNNING"
                onClicked: if (python) python.stop_task("winaqms_publisher")
            }
        }
    }
    
    // Handle cronjobs state changes
    Connections {
        target: python
        function onServiceStateChanged(serviceId, state) {
            // Actualizar el estado correspondiente
            if (serviceId === "csv_publisher") {
                csvPublisherState = state
            } 
            if (serviceId === "winaqms_publisher") {
                winaqmsPublisherState = state
            }
        }
    }
}
