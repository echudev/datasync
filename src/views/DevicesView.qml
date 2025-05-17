// Importación de módulos necesarios de Qt
import QtQuick 2.15
import QtQuick.Controls 2.15
import QtQuick.Layouts 1.15
import Theme 1.0

// Vista de Servicios: fondo con etiqueta centrada
Column {
    anchors.fill: parent
    spacing: 20
    
    // Propiedades para mantener el estado de los dispositivos
    property string dataCollectorState: python ? python.get_service_state("data_collector") : "STOPPED"

    Rectangle {
        width: parent.width
        height: 100
        color: dataCollectorState === "RUNNING" ? Theme.deviceRunningBkg : Theme.deviceStoppedBkg
        border.color: dataCollectorState === "RUNNING" ? Theme.deviceRunningBorder : Theme.deviceStoppedBorder
        radius: 5
        
        RowLayout {
            anchors.fill: parent
            anchors.margins: 10
            spacing: 10
            ColumnLayout {
                Layout.fillWidth: true
                Label {
                    text: "Meteo: Davis VantagePro2"
                    font.bold: true
                }
                Label {
                    text: "Recolecta datos meteorológicos"
                    font.pixelSize: 12
                }
                Label {
                    text: "[Dir.viento, Vel.viento, temperatura, humedad, Presión ATM, Lluvia]"
                    font.pixelSize: 10
                }
            }
            Button {
                text: "INICIAR"
                enabled: dataCollectorState === "STOPPED"
                onClicked: if (python) python.start_task("data_collector")
            }
            Button {
                text: "DETENER"
                enabled: dataCollectorState === "RUNNING"
                onClicked: if (python) python.stop_task("data_collector")
            }
        }
    }

    
    // Handle devices state changes
    Connections {
        target: python
        function onServiceStateChanged(serviceId, state) {
            // Actualizar el estado correspondiente
            if (serviceId === "data_collector") {
                dataCollectorState = state
            }
        }
    }
}
