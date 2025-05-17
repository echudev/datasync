// Importación de módulos necesarios de Qt
import QtQuick 2.15
import QtQuick.Controls 2.15
import QtQuick.Layouts 1.15
import Theme 1.0

// Vista principal de tareas: contiene título, botones y área de logs
ColumnLayout {
    Layout.fillWidth: true
    Layout.fillHeight: true
    // Área de logs: muestra los mensajes generados por las tareas
    Rectangle {
        Layout.fillWidth: true
        Layout.fillHeight: true
        color: Theme.secondary
        border.color: Theme.accent
        border.width: 2
        radius: 8

        ScrollView {
            anchors.fill: parent
            anchors.margins: 8

            TextArea {
                id: logArea
                anchors.fill: parent
                anchors.margins: 8
                readOnly: true
                placeholderText: "Acá aparecerán los mensajes de la aplicacion"
                color: "white"
                background: null // Usa el fondo del Rectangle
                // Al crearse, carga el log actual desde Python
                // Component.onCompleted: {
                //     if (python) logArea.text = python.logText;
                // }
            }
        }
    }
    // Fila de botones para borrar y exportar el log
    RowLayout {
        Layout.fillWidth: true
        Layout.margins: 8
        spacing: 10
        Layout.alignment: Qt.AlignHCenter
        // Botón para limpiar el log
        Button {
            text: "Limpiar"
            onClicked: python.clearLog()
        }
        // Botón para exportar el log
        Button {
            text: "Exportar"
           // onClicked: python.exportLog()
        }
    }

    // Conexión para actualizar el área de logs cuando cambia el log en Python
    Connections {
        target: python
        function onLogTextChanged() {
            logArea.text = python.logText
        }
    }
}
