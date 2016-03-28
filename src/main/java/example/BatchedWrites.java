package example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;

// Takes inbound cypher statements, executes them in batches in the background, giving us
// nice performance for async writes.
public class BatchedWrites
{
    // This is not available by default, we've defined this further down in the class,
    // and use a kernel extension to register it for procedure injection.
    @Context
    public BatchedWriter writer;

    /**
     * This will queue up a write to be batched - but it will also wait for that write to complete,
     * this gives somewhat nicer client semantics, because we know that when the statement returns
     * the write has happened. The drawback is that we (obviously) get higher latencies since we now
     * wait for the actual write, and we only batch things ran concurrently across sessions, not
     * statements in the same session.
     *
     * @param statement
     * @param parameters
     */
    @Procedure("example.batch.write")
    @PerformsWrites
    public void write(
            @Name("statement") String statement,
            @Name("parameters") Map<String, Object> parameters ) throws InterruptedException
    {
        // We use a latch to get signalled when the write is completed
        CountDownLatch completed = new CountDownLatch(1);

        // Tell the batch writer to do its thing
        writer.schedule( statement, parameters, completed);

        // Wait for the statement to get written
        if( !completed.await(30, TimeUnit.SECONDS) )
        {
            throw new RuntimeException( "Waited 30 seconds for write to complete, giving up." );
        }
    }

    /**
     * Unlike {@link #write(String, Map)}, this will *not* wait for the batch writer to tell us
     * the write was completed. This means we 1) get much lower latencies and 2) end up batching writes
     * within sessions as well as across sessions.
     *
     * The drawback is we need to use polling to tell when our statement has been executed, and we don't
     * find out if our statement fails :(
     *
     * @param statement
     * @param parameters
     */
    @Procedure("example.batch.writeAsync")
    @PerformsWrites
    public void writeAsync(
            @Name("statement") String statement,
            @Name("parameters") Map<String, Object> parameters )
    {
        writer.schedule( statement, parameters, null );
    }

    /**
     * This is the main part of this component - it runs as a background job that grabs as much
     * as it can off of a queue and batches those operations together in transactions.
     */
    public static class BatchedWriter implements Runnable
    {
        public static final int MAX_BATCH_SIZE = 100;
        // LinkedTransferQueue is new in 1.7, and is supposed to be several x faster than ABQ for
        // job-transfer type workloads. I'm not sure if that's true for the batching style we're doing
        // here though, it could be ABQ performs better when moving lots of queue items at a time.
        private final LinkedTransferQueue<Statement> pending = new LinkedTransferQueue<>();

        // By using KernelExtension, we get access to the "real" GDB, rather than the context-specific
        // instance procedures normally are given.
        private final GraphDatabaseAPI db;

        // To allow gracefully stopping this writer
        private volatile boolean stopped = true;

        public BatchedWriter( GraphDatabaseAPI db )
        {
            this.db = db;
        }

        public void schedule( String statement, Map<String,Object> parameters, CountDownLatch onComplete )
        {
            pending.add( new Statement( statement, parameters, onComplete ) );
        }

        @Override
        public void run()
        {
            try
            {
                List<Statement> currentBatch = new ArrayList<>();
                while ( !stopped )
                {
                    Statement stmt = pending.poll( 500, TimeUnit.MILLISECONDS );
                    if( stmt == null )
                    {
                        // We get null back if nothing arrives within our timeout above - which we
                        // want, because we want to check the `stopped` flag every second or so,
                        // to ensure we don't hold up shutdown.
                        continue;
                    }

                    try ( Transaction tx = db.beginTx() )
                    {
                        // This loop could be replaced with drainTo() if we used ABQ, for LTQ
                        // it might be interesting to explore as well, since that'd read up all
                        // the statements in a block, rather than trash the CPU caches in between
                        // each call to #poll() as we do below.
                        int n = 0;
                        do
                        {
                            currentBatch.add( stmt );
                            // Obviously this is missing proper error handling
                            db.execute( stmt.text, stmt.parameters );
                        }
                        while( n++ < MAX_BATCH_SIZE && (stmt=pending.poll()) != null );
                        tx.success();
                    }

                    // After the transaction, signal each statement completed
                    currentBatch.forEach( Statement::complete );
                    currentBatch.clear();
                }
            }
            catch( Exception e )
            {
                // Again - obviously this is insufficient. The code is missing handling a single
                // statement failing in a batch, and implementing appropriate retries when that
                // happens for the other statements in the group.
                e.printStackTrace();
            }
        }

        public void start() { stopped = false; }
        public void stop() { stopped = true; }
    }

    // This gets registered with neo via the META-INF/services/blah.blah file.
    public static class BatchedWriterFactory extends KernelExtensionFactory<BatchedWriterFactory.Dependencies>
    {
        interface Dependencies
        {
            GraphDatabaseAPI graphDatabase();
            Procedures procedures();
            JobScheduler scheduler();
        }

        public BatchedWriterFactory()
        {
            super( "batched-write-service" );
        }

        @Override
        public Lifecycle newInstance( KernelContext kernelContext, Dependencies dependencies ) throws Throwable
        {
            BatchedWriter writer = new BatchedWriter(dependencies.graphDatabase());

            // Register the writer for injection
            dependencies.procedures().registerComponent( BatchedWriter.class, (ctx) -> writer );

            // We use the database job scheduler, mostly because that makes our threads look
            // like they fit in with other neo threads in stack traces, but it also means we don't
            // have to think about thread scheduling edge cases.
            JobScheduler.Group scheduleGroup = new JobScheduler.Group( "BatchedWriter", NEW_THREAD );

            return new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    writer.start();
                    dependencies.scheduler().schedule( scheduleGroup, writer );
                }

                @Override
                public void stop() throws Throwable
                {
                    writer.stop();
                }
            };
        }
    }

    public static class Statement
    {
        private final String text;
        private final Map<String, Object> parameters;
        private final CountDownLatch onComplete;

        public Statement( String text, Map<String,Object> parameters, CountDownLatch onComplete )
        {
            this.text = text;
            this.parameters = parameters;
            this.onComplete = onComplete;
        }

        public void complete()
        {
            if(onComplete != null)
            {
                onComplete.countDown();
            }
        }
    }
}
