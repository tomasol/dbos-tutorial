package com.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

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

    public int parallelParent() throws Exception;
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
            System.out.printf("Step succeeded %d==%d%n", i, result);

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
    public int parallelParent() throws Exception {
        System.out.println("parallel-parent started");
        ArrayList<Map.Entry<Integer, WorkflowHandle<Integer, Exception>>> handles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            WorkflowHandle<Integer, Exception> handle = DBOS.startWorkflow(
                    () -> this.proxy.childWorkflow(index),
                    new StartWorkflowOptions().withQueue(this.queue));
            handles.add(new AbstractMap.SimpleEntry<>(i, handle)); // Tuple (i, handle)
        }
        System.out.println("parallel-parent submitted all parallel-child workflows");
        int acc = 0;
        for (var handle : handles) {
            System.out.printf("Awaiting parallel-child workflow %d%n", handle.getKey());
            int result = handle.getValue().getResult();
            acc += result;
            System.out.printf("parallel-child succeeded %d%n", result);
        }
        System.out.println("parallel-parent completed");
        return acc;
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
                    int acc = proxy.parallelParent();
                    ctx.result("parallel workflow completed: " + acc);
                })
                .start(9000);
    }
}
