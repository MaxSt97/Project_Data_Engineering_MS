# Project_Data_Engineering_DLMDWWDE02
**Hintergrund**
- Zu erstellen war eine Batch-Pipeline, welche für eine datenintensive Applikation mindestens eine Millionen Datensätze zu einem fiktiven Use_Case einließt, verarbeitet, speichert.
- Diese Daten sollen im weiteren Verlauf, welcher nicht Teil dieses Moduls ist, für die Aktualisierung eines ML-Alogirhtmus genutzt werden.

**Anforderungen Use-Case**
- Zu berücksichtigende Aspekte waren die Verlässlichkeit, Skalierbarkeit und Wartbarkeit des Systems.
- Die Versionierung des Codes sollte über Github erfolgen.
- Im Sinne der Microservice Architektur sollte auf das Prinzip der Containerisierung zurückgegriffen werden. 

**Anforderungen Host-System**
- Auf dem Hostsystem sollte Docker installiert sein, idealerweise mit der Docker Desktop komponente. Diese dient der visuellen Überwachung der Container. Auch können über diesen die Logs der Container eingesehen werden.
- Das gesamte Repository muss im selben Verzeichnis des Hostsystems liegen. Hierzu muss auch folgende CSV-Rohdatei heruntergeladen werden. Auch diese muss im besagten Verzeichnis liegen. [Data](https://file.io/JevmUYhxdr18)
- Die Installation von Python ist nicht notwendig, da dies bereits über die jeweiligen Container bereitgestellt.

**Architektur**
![Skizze der Datenarchitektur](https://github.com/MaxSt97/Project_Data/assets/105374626/e9be2073-2e3e-4a6f-aa5e-cae355196747)

**Ausführen der Pipeline**

1. Im lokalen Verzeichnis "CMD" ausführen. Im Ordner "Project Data Engineering II IU" liegen beispeilsweise alle Dateien. 
```
C:\Users\PycharmProjects\Project Data Engineering II IU>
```
2. Docker-Container Images erstellen:
```
docker-compose build --no-cache
```
3. Pipeline Ausführen.
```
docker-compose up
```
4. Pipeline führt alle zuvor definierten Schritte aus. Die angezeigten Logs in CMD geben Auskunft über den aktuellen Status. Es wurden verschiedene Informationen für den Endnutzer hinzugefügt. Beispielsweise:
```
jupyter-pyspark-notebook               | Verbindung zur PostgreSQL-Datenbank cleaned_data erfolgreich hergestellt.
jupyter-pyspark-notebook               | Es wurden 63417 Ausreißer entfernt.
jupyter-pyspark-notebook               | Schreiben der Daten in die Tabelle cleaned_data erfolgreich.
```

**Insight Plots**

![Trip_Distance_vs_Fare_Amount_Color-coded_by_Trip_Duration](https://github.com/MaxSt97/Project_Data/assets/105374626/19828baf-5526-4667-9701-c1e7bbf84874)
![Fahrten_pro_Stunde](https://github.com/MaxSt97/Project_Data/assets/105374626/e46c767a-f681-40bb-97fb-9939d8ff79f4)
![Umsatz_nach_VendorID](https://github.com/MaxSt97/Project_Data/assets/105374626/74d422d7-04f0-457d-81ab-449921b7ab56)

