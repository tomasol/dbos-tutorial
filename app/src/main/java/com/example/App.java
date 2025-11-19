package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    public void setProxy(Example proxy);

    public void taskWorkflow(int i) throws Exception;

    public void parentSerial() throws Exception;

    public void parentParallel() throws Exception;
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

    @Workflow(name = "task-workflow")
    public void taskWorkflow(int i) throws Exception {
        System.out.printf("Task %d started%n", i);
        Thread.sleep(i * 300);
        System.out.printf("Task %d creating file%n", i);
        java.nio.file.Files.createFile(java.nio.file.Path.of("file-" + i + ".txt"));
        System.out.printf("Task %d completed%n", i);
    }

    @Workflow(name = "parent-serial")
    public void parentSerial() throws Exception {
        System.out.println("parent-serial started");
        for (int i = 0; i < 10; i++) {
            final int i2 = i;
            try {
                DBOS.runStep(() -> taskWorkflow(i2), "step " + i);
            } catch (Exception e) {
                System.out.println("caught " + e);
            }
        }
        System.out.println("parent-serial completed");
    }

    @Workflow(name = "parent-parallel")
    public void parentParallel() throws Exception {
        System.out.println("parent-parallel started");
        List<WorkflowHandle<Void, Exception>> handles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            WorkflowHandle<Void, Exception> handle = DBOS.startWorkflow(
                    () -> this.proxy.taskWorkflow(index),
                    new StartWorkflowOptions().withQueue(this.queue));
            handles.add(handle);
        }
        System.out.println("parent-parallel submitted all child tasks");
        for (WorkflowHandle<Void, Exception> handle : handles) {
            try {
                handle.getResult();
            } catch (Exception e) {
                System.out.println("Task failed " + e);
            }
        }
        System.out.println("parent-parallel completed");
    }
}

public class App {
    public static void main(String[] args) throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.ERROR);
        DBOSConfig config = DBOSConfig.defaults("java1")
                // .withConductorKey(System.getenv("CONDUCTOR_KEY"))
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
                    proxy.parentSerial();
                    ctx.result("Workflow executed!");
                })
                .get("/parallel", ctx -> {
                    proxy.parentParallel();
                    ctx.result("Workflow executed!");
                })
                .start(8080);
    }
}
