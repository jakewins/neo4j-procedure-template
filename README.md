# Neo4j Procedure Template

This project is an example you can use to build Procedures in Neo4j.
It contains two procedures, for reading and updating a full-text index.

To try this out, simply clone this repository and have a look at the
source code under `src/main/java/example/FullTextIndex`.

## Building

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file,`target/procedure-template-1.0.0-SNAPSHOT.jar`,
that can be deployed in the `plugin` directory of your Neo4j instance.

## License

Apache License V2, see LICENSE