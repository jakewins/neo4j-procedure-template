package example;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWriteOperations;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Resource;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class FullTextIndex
{
    public static final Map<String,String> FULL_TEXT = stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );

    @Resource
    public GraphDatabaseService db;

    @Procedure
    public Stream<SearchHit> search( @Name("label") String label,
                                     @Name("query") String query )
    {
        if( !db.index().existsForNodes( indexName( label ) ))
        {
            return Stream.empty();
        }

        // TODO pending PR we can remove this spliterator boilerplate
        return stream( spliteratorUnknownSize(
            db.index()
            .forNodes( indexName( label ), FULL_TEXT )
            .query( query ), 0 ), false )
            .map( SearchHit::new );
    }

    @Procedure
    @PerformsWriteOperations
    public Stream<SearchHit> index( @Name("nodeId") long nodeId,
                       @Name("properties") List<String> propKeys )
    {
        Node node = db.getNodeById( nodeId );

        // Load all properties for the node once and in bulk,
        // the resulting set will only contain those properties in `propKeys`
        // that the node actually contains.
        Set<Map.Entry<String,Object>> properties = node.getProperties( propKeys.toArray( new String[0] ) ).entrySet();

        // Index every label (this is just as an example, we could filter which labels to index)
        for ( Label label : node.getLabels() )
        {
            Index<Node> index = db.index().forNodes( indexName( label.name() ), FULL_TEXT );

            // In case the node is indexed before, remove all occurrences of it so
            // we don't get old or duplicated data
            index.remove( node );

            // And then index all the properties
            for ( Map.Entry<String,Object> property : properties )
            {
                index.add( node, property.getKey(), property.getValue() );
            }
        }

        return Stream.empty(); // TODO pending void PR
    }

    private String indexName( @Name( "label" ) String label )
    {
        return "label-" + label;
    }

    public static class SearchHit
    {
        public Node node;

        public SearchHit( Node node )
        {
            this.node = node;
        }

    }
}
