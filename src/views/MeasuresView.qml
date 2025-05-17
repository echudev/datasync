// Importación de módulos necesarios de Qt
import QtQuick 2.15
import QtQuick.Controls 2.15

// Vista de Medidas: fondo con etiqueta centrada
Rectangle {
    color: "#00000022" // Fondo semitransparente
    radius: 10 // Bordes redondeados
    anchors.fill: parent // Ocupa todo el espacio disponible
    // Etiqueta principal de la vista
    Label {
        text: "Vista de Medidas"
        anchors.centerIn: parent
        color: "white"
        font.pixelSize: 18
    }
}
