package netmonitor;

/**
 * Antarmuka "kejadian" antara mesin jaringan klien dan lapisan tampilan.
 *
 * Mesin jaringan ({@link ClientEngine}) TIDAK pernah mencetak ke layar, membaca keyboard, atau memanggil
 * System.exit. Semua yang perlu diketahui pengguna dilaporkan lewat method di bawah ini, dan tampilan
 * (konsol, atau GUI web) memutuskan cara menampilkannya. Method dipanggil dari thread milik mesin,
 * jadi implementasi GUI wajib memindahkan pembaruan layar ke thread UI-nya.
 */
public interface ClientListener {

    /** Informasi biasa. */
    void onInfo(String message);

    /** Galat atau peringatan yang dapat dibaca pengguna. */
    void onError(String message);

    /**
     * Login berhasil.
     *
     * @param loginResponse isi respons server, mis. "200 OK Login Berhasil, lokasi: ..."
     * @param reconnected   true bila ini hasil sambung ulang otomatis (khusus Agent)
     */
    void onConnected(String loginResponse, boolean reconnected);

    /** Baris dari server yang bukan jawaban sebuah perintah (mis. pemberitahuan). */
    void onServerMessage(String line);

    /**
     * Koneksi terputus.
     *
     * @param reason        penyebab yang sudah berupa kalimat mudah dibaca
     * @param willReconnect true bila mesin akan mencoba menyambung ulang
     * @param byServer      true bila server sendiri yang memberi tahu (dimatikan, sesi habis)
     */
    void onConnectionLost(String reason, boolean willReconnect, boolean byServer);

    /** Mesin menunggu sebelum percobaan sambung ulang ke-{@code attempt} dari {@code maxAttempts}. */
    void onReconnecting(int attempt, int maxAttempts, long delayMs);
}
