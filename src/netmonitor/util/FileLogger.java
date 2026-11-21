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

    /** Formatter timestamp yang digunakan pada setiap baris log. */
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Penulis buffered ke berkas log. */
    private final BufferedWriter writer;
    /** Flag apakah baris log juga dicetak ke konsol. */
    private final boolean echoToConsole;
    /** Flag apakah writer sudah ditutup. */
    private boolean closed;

    /**
     * Membuka (atau membuat) berkas log dalam mode append.
     *
     * @param path          path berkas log yang akan ditulis
     * @param echoToConsole bila true, setiap baris log juga dicetak ke konsol
     * @throws IOException bila berkas tidak dapat dibuka
     */
    public FileLogger(String path, boolean echoToConsole) throws IOException {
        // append = true: log lama tidak ditimpa saat server dijalankan ulang
        this.writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path, true), StandardCharsets.UTF_8));
        this.echoToConsole = echoToConsole;
    }

    /** @param message pesan bertingkat INFO. */
    public void info(String message) {
        write("INFO", message);
    }

    /** @param message pesan bertingkat WARN. */
    public void warn(String message) {
        write("WARN", message);
    }

    /** @param message pesan bertingkat ERROR. */
    public void error(String message) {
        write("ERROR", message);
    }

    /**
     * Menulis satu baris log dengan timestamp, level, dan nama thread.
     *
     * @param level   level log (INFO/WARN/ERROR)
     * @param message isi pesan
     */
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

    /** Menutup penulis log. Setelah ditutup, pemanggilan {@code write} diabaikan. */
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
