Projet Big Data - Spark
=====
##### Par HADJI Mohamed Allam (5JAL)
*****

### Prérequis

* scala 2.12
* spark 2.4.5
* maven .*

### Installation du projet

```shell script
mvn clean install
```

### Génération du JAR

```shell script
mvn -DskipTests clean package
```

### Lancement du projet
```shell script
spark-submit \
  --class esgi.exo.FootballApp \
  --master local \
  <jar-path> \
  <input-file-path>
```