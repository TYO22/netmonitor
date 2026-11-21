package netmonitor.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Pencatat log ke berkas fisik (.log) memakai Buffered stream (Pertemuan 2).
 * Semua method synchronized agar baris log dari banyak thread tidak tercampur.
 */
public final class FileLogger {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final BufferedWriter writer;
    private final boolean echoToConsole;
    private boolean closed;

    public FileLogger(String path, boolean echoToConsole) throws IOException {
        // append = true: log lama tidak ditimpa saat server dijalankan ulang
        this.writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path, true), StandardCharsets.UTF_8));
        this.echoToConsole = echoToConsole;
    }

    public void info(String message) {
        write("INFO", message);
    }

    public void warn(String message) {
        write("WARN", message);
    }

    public void error(String message) {
        write("ERROR", message);
    }

    private synchronized void write(String level, String message) {
        String line = "[" + LocalDateTime.now().format(TS) + "] [" + level + "] ["
                + Thread.currentThread().getName() + "] " + message;
        if (echoToConsole) {
            System.out.println(line);
        }
        if (closed) {
            return;
        }
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            // kegagalan log tidak boleh menjatuhkan server
            System.err.println("Gagal menulis log: " + e.getMessage());
        }
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writer.close();
        } catch (IOException ignored) {
            // tidak ada yang bisa dilakukan
        }
    }
}
