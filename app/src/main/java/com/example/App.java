package com.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.LoggerFactory;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;
import io.javalin.Javalin;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

interface Example {

    public int serial() throws Exception;

    public int childWorkflow(int i) throws Exception;

    public long parallelParent() throws Exception;

    public void sleepyWorkflow(int i);

    public void sleepyParent(int max);

}

class ExampleImpl implements Example {

    private final Queue queue;
    private Example proxy;

    public ExampleImpl(Queue queue) {
        this.queue = queue;
    }

    public void setProxy(Example proxy) {
        this.proxy = proxy;
    }

    // Idempotent activity
    private static int step(int idx, int sleepMillis) throws Exception {
        System.out.printf("Step %d started%n", idx);
        Thread.sleep(sleepMillis);
        System.out.printf("Step %d creating file%n", idx);
        Path path = Path.of("file-" + idx + ".txt");
        Files.write(path, new byte[0]);
        System.out.printf("Step %d completed%n", idx);
        return idx;
    }

    @Workflow(name = "serial")
    public int serial() throws Exception {
        System.out.println("serial started");
        int acc = 0;
        for (int i = 0; i < 10; i++) {
            System.out.println("Persistent sleep started");
            DBOS.sleep(Duration.ofSeconds(1));
            System.out.println("Persistent sleep finished");
            final int i2 = i;
            int result = DBOS.runStep(() -> step(i2, 200 * i2), "step " + i);
            acc += result;
            System.out.printf("step(%d)=%d%n", i, result);

        }
        System.out.println("serial completed");
        return acc;
    }

    // This child workflow exists because there is no way to run steps directly in
    // parallel.
    @Workflow(name = "parallel-child")
    public int childWorkflow(int i) throws Exception {
        return DBOS.runStep(() -> step(i, 200 * i), "step " + i);
    }

    @Workflow(name = "parallel-parent")
    public long parallelParent() throws Exception {
        System.out.println("parallel-parent started");
        HashSet<Map.Entry<Integer, WorkflowHandle<Integer, Exception>>> handles = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            var handle = DBOS.startWorkflow(
                    () -> this.proxy.childWorkflow(index),
                    new StartWorkflowOptions().withQueue(this.queue));
            handles.add(new AbstractMap.SimpleEntry<>(i, handle)); // Tuple (i, handle)
        }
        System.out.println("parallel-parent submitted all parallel-child workflows");
        long acc = 0;
        for (var entry : handles) {
            int result = entry.getValue().getResult();
            acc = 10 * acc + result; // Order-sensitive
            int i = entry.getKey();
            System.out.printf("parallel-child(%d)=%d, acc:%d%n", i, result, acc);
            DBOS.sleep(Duration.ofMillis(300));
        }
        System.out.printf("parallel-parent completed: %d%n", acc);
        return acc;
    }

    @Workflow
    public void sleepyWorkflow(int idx) {
        System.out.printf("%d%n", idx);
        DBOS.sleep(Duration.ofDays(1));
        // do some logic here
    }

    @Workflow
    public void sleepyParent(int max) {
        var handles = new ArrayList<WorkflowHandle<Void, RuntimeException>>();
        for (int i = 0; i < max; i++) {
            final int index = i;
            System.out.printf("Submitting child workflow %d%n", i);
            var handle = DBOS.startWorkflow(
                    () -> this.proxy.sleepyWorkflow(index),
                    new StartWorkflowOptions().withQueue(this.queue));
            handles.add(handle);
        }
        System.out.printf("Created %s child workflows%n", max);
        int counter = 0;
        for (var handle : handles) {
            handle.getResult();
            counter++;
            System.out.printf("Collected %d child workflows%n", counter);
        }
        System.out.printf("Done waiting for %d child workflows%n", max);
    }
}

public class App {
    public static void main(String[] args) throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.ERROR);
        DBOSConfig config = DBOSConfig.defaults("java1")
                // .withConductorKey(System.getenv("CONDUCTOR_KEY"))
                .withAppVersion("test-app-version") // Allow changing the code when replaying workflows
                .withDatabaseUrl(System.getenv("DBOS_SYSTEM_JDBC_URL"))
                .withDbUser(System.getenv("PGUSER"))
                .withDbPassword(System.getenv("PGPASSWORD"));
        DBOS.configure(config);
        Queue queue = new Queue("example-queue");
        DBOS.registerQueue(queue);
        ExampleImpl impl = new ExampleImpl(queue);
        Example proxy = DBOS.registerWorkflows(Example.class, impl);
        impl.setProxy(proxy);
        DBOS.launch();
        Javalin.create()
                .get("/serial", ctx -> {
                    int acc = proxy.serial();
                    ctx.result("serial workflow completed: " + acc);
                })
                .get("/parallel", ctx -> {
                    long acc = proxy.parallelParent();
                    ctx.result("parallel workflow completed: " + acc);
                })
                .get("/sleep", ctx -> {
                    proxy.sleepyParent(10000);
                })
                .start(9000);
    }
}
