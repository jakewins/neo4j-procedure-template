package example;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.MapUtil.map;

public class LegacyFullTextIndexTest
{
    @Rule
    public Neo4jRule neo4j = (Neo4jRule)new Neo4jRule() // TODO: temp cast pending PR
            .withProcedure( FullTextIndex.class );

    @Test
    public void shouldAllowIndexingAndFindingANode() throws Throwable
    {
        // Given I've started Neo4j with the FullTextIndex procedure class
        //       which my 'neo4j' rule above does.
        GraphDatabaseService db = neo4j.getGraphDatabaseService();

        // And given I have a node in the database
        long nodeId = (long)db
                    .execute( "CREATE (p:User {name:'Steven Brookreson'}) RETURN id(p)" )
                    .columnAs( "id(p)" )
                    .next();

        // When I use the index procedure to index a node
        db.execute( "CALL example.index({id}, ['name'])", map( "id", nodeId ) )
                .hasNext(); // TODO temp until eagerization

        // Then I can search for that node with fuzzy keywords
        Result id = db.execute( "CALL example.search('User', 'name:Brook*')", map( "id", nodeId ) );

        assertThat( id.stream().count(), equalTo( 1l ));
    }
}
