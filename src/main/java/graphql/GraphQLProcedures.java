package graphql;

import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;
import static org.neo4j.helpers.collection.MapUtil.map;

public class GraphQLProcedures
{
    private static final Map<String, String> fields = new HashMap<>();
    private static GraphQLSchema schema;

    @Context
    public GraphDatabaseService db;

    @Procedure
    public Stream<GraphQLResult> query( @Name("query") String query )
    {
        ExecutionResult result = new GraphQL( schema )
                .execute( query, map( "db", db ) );

        Object data = result.getData();
        if( data instanceof Map )
        {
            return Stream.of( new GraphQLResult( (Map)data ) );
        }
        else
        {
            throw new IllegalStateException( "Unknown result type: `"+data.getClass()+"`" );
        }
    }

    @Procedure
    public void createQueryField(
            @Name( "name" ) String name,
            @Name( "cypherQuery" ) String cypherQuery )
    {
        synchronized ( fields )
        {
            String old = fields.putIfAbsent( name, cypherQuery );
            if( old != null )
            {
                throw new IllegalStateException( "`" + name + "` is already in use." );
            }

            rebuildSchema();
        }
    }

    private void rebuildSchema()
    {
        GraphQLSchema.Builder schemaBuilder = GraphQLSchema.newSchema();
        GraphQLObjectType.Builder rootQuery = newObject().name( "queryRoot" );

        for ( Map.Entry<String,String> field : fields.entrySet() )
        {
            rootQuery.field( newFieldDefinition()
                    .type(GraphQLString)
                    .name(field.getKey())
                    .dataFetcher( (env) ->
                    {
                        GraphDatabaseService db =
                                (GraphDatabaseService) ((Map<String, Object>)env.getContext()).get( "db" );

                        Result result = db.execute( field.getValue() );
                        if(result.hasNext())
                        {
                            return result.next().values().iterator().next().toString();
                        }
                        else
                        {
                            return "N/A";
                        }
                    })
                    .build());
        }


        schema = schemaBuilder
                .query(rootQuery.build())
                .build();
    }

    public static class GraphQLResult
    {
        public Map<String, Object> output;

        public GraphQLResult( Map output )
        {
            this.output = output;
        }
    }
}
