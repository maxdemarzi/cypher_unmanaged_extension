Neo4j Cypher Unmanaged Extension
================================

This is an unmanaged extension translating Cypher queries. 

1. Build it: 

        mvn clean package

2. Copy target/cypher-neo4jextension-1.0.jar to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.cypher.neo4jextension=/example

4. Start Neo4j server.

5. Query it over HTTP:

        curl http://localhost:7474/example/helloworld

