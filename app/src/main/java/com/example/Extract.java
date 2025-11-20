package com.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import dev.dbos.transact.DBOS;

public class Extract {

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

    public static void serial() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("Persistent sleep started");
            DBOS.sleep(Duration.ofSeconds(1));
            System.out.println("Persistent sleep finished");
            final int i2 = i;
            int result = DBOS.runStep(() -> step(i2, 200 * i2), "step " + i);
            System.out.printf("Step succeeded %d==%d%n", i, result);

        }

    }
}
