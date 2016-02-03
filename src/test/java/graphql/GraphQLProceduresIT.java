package graphql;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import static org.neo4j.bolt.BoltKernelExtension.Settings.connector;
import static org.neo4j.bolt.BoltKernelExtension.Settings.enabled;

public class GraphQLProceduresIT
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure( GraphQLProcedures.class )

            // Temporary until Neo4jRule includes Bolt by default
            .withConfig( connector( 0, enabled ), "true" );

    @Test
    public void shouldRunHelloWorldExample() throws Throwable
    {
        System.in.read();
        // Given
        try( Driver driver = GraphDatabase.driver( "bolt://localhost" ) )
        {
            Session session = driver.session();

            // Register a cypher query as a GraphQL field
            session.run( "CALL graphql.createQueryField('hello', 'RETURN \"hello, world!\"')" );

            // And then run a graphQL query against the updated schema
            System.out.println(
                    session.run( "CALL graphql.query('{hello}')" )
                    .single()
                    .get(0).asMap());

        }
    }
}
