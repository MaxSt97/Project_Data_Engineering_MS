# Project_Data_Engineering_DLMDWWDE02
*Hintergrund*
- Zu erstellen war eine Batch-Pipeline, welche für eine datenintensive Applikation mindestens eine Millionen Datensätze zu einem fiktiven Use_Case einließt, verarbeitet, speichert.
- Diese Daten sollen im weiteren Verlauf, welcher nicht Teil dieses Moduls ist, für die Aktualisierung eines ML-Alogirhtmus genutzt werden.

*Anforderungen Use-Case*
- Zu berücksichtigende Aspekte waren die Verlässlichkeit, Skalierbarkeit und Wartbarkeit des Systems.
- Die Versionierung des Codes sollte über Github erfolgen.
- Im Sinne der Microservice Architektur sollte auf das Prinzip der Containerisierung zurückgegriffen werden. 

*Anforderungen Host-System*
- Auf dem Hostsystem sollte Docker installiert sein, idealerweise mit der Docker Desktop komponente. Diese dient der visuellen Überwachung der Container. Auch können über diesen die Logs der Container eingesehen werden.
- Das gesamte Repository muss im selben Verzeichnis des Hostsystems liegen. Hierzu muss auch folgende CSV-Rohdatei heruntergeladen werden. Auch diese muss im besagten Verzeichnis liegen. [Data](https://file.io/JevmUYhxdr18)
- Die Installation von Python ist nicht notwendig, da dies bereits über die jeweiligen Container bereitgestellt.

*Architektur*
![Skizze der Datenarchitektur](https://github.com/MaxSt97/Project_Data/assets/105374626/e9be2073-2e3e-4a6f-aa5e-cae355196747)

*Ausführen der Pipeline* 
