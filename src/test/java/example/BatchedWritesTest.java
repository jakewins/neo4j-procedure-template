package example;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.v1.Values.parameters;

public class BatchedWritesTest
{
    // This rule starts a Neo4j instance for us
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure we want to test
            .withProcedure( BatchedWrites.class );

    @Test
    public void shouldPerformBatchedWrites() throws Throwable
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given I've started Neo4j with the FullTextIndex procedure class
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I perform a write through the batch writer
            session.run( "CALL example.batch.write", parameters(
                    "statement", "CREATE ({ name:{name} })",
                    "parameters", parameters( "name", "Jim-Bob" )) );

            // Then it should immediately show up in the next query
            String name = session.run( "MATCH (n) RETURN n.name" ).single().get( "n.name" ).asString();
            assertEquals("Jim-Bob", name);
        }
    }
}
