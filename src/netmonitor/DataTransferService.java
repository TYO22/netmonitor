package netmonitor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Layanan kanal data TCP terpisah untuk transfer berkas biner (Pilar 3).
 *
 * <p>Kanal data memakai port yang dapat dikonfigurasi lewat properti sistem
 * {@code netmonitor.data.port} (default 9092). Setiap transfer diawali dengan
 * token sekali pakai yang dibuat lewat kanal perintah, lalu dikirim klien/Agent
 * sebagai {@code writeUTF(token)} pada soket data. Dengan demikian soket perintah
 * utama tidak pernah bercampur dengan aliran biner dan tidak terjadi bentrok
 * antara {@code BufferedReader} (teks) dan {@code DataInputStream} (biner).</p>
 *
 * <p>Format aliran data:</p>
 * <ol>
 *   <li>{@code writeUTF(token)} dari klien/Agent (autentikasi transfer)</li>
 *   <li>{@code writeUTF(namaBerkas)} + {@code writeLong(ukuran)} sebagai header metadata</li>
 *   <li>Potongan data 8 KB (8192 byte) dengan buffered stream</li>
 *   <li>{@code shutdownOutput()} pada soket data sebagai penanda akhir (EOF)</li>
 *   <li>Verifikasi ukuran akhir; berkas parsial dihapus bila ukuran tidak cocok</li>
 * </ol>
 */
public class DataTransferService {

    private static final int CHUNK_SIZE = 8192;
    private static final long TOKEN_TIMEOUT_MS = 300_000; // 5 menit

    private final int port;
    private final String uploadsDir;
    private final FileLogger log;
    private final ConcurrentHashMap<String, TransferToken> tokens = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final ScheduledExecutorService cleanupScheduler;
    private ServerSocket serverSocket;
    private volatile boolean running;

    /** State satu transfer aktif. Token bersifat sekali pakai (lihat {@link #consumed}). */
    static final class TransferToken {
        final String filename;
        long expectedSize;
        final boolean isUpload;
        final long startTime;
        final AtomicBoolean consumed = new AtomicBoolean(false);
        final CompletableFuture<String> result = new CompletableFuture<>();

        TransferToken(String filename, long expectedSize, boolean isUpload) {
            this.filename = filename;
            this.expectedSize = expectedSize;
            this.isUpload = isUpload;
            this.startTime = System.currentTimeMillis();
        }
    }

    /**
     * Membuat layanan kanal data.
     *
     * @param port       port TCP untuk kanal data (mis. 9092)
     * @param uploadsDir direktori penyimpanan berkas unggahan
     * @param log        pencatat log server
     */
    public DataTransferService(int port, String uploadsDir, FileLogger log) {
        this.port = port;
        this.uploadsDir = uploadsDir;
        this.log = log;
        this.pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "DataTransfer-" + new Random().nextInt(10000));
            t.setDaemon(true);
            return t;
        });
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "token-cleanup");
            t.setDaemon(true);
            return t;
        });
    }

    /** @return port TCP kanal data yang dipakai layanan ini. */
    public int getPort() {
        return port;
    }

    /**
     * Membuat token sekali pakai untuk transfer UPLOAD (Inspector mengirim berkas ke server).
     * Ukuran berkas akan dibaca dari header yang dikirim klien.
     *
     * @param filename nama berkas yang akan disimpan di direktori unggahan
     * @return token unik untuk transfer ini
     */
    public String createUploadToken(String filename) {
        String token = UUID.randomUUID().toString();
        tokens.put(token, new TransferToken(filename, 0L, true));
        log.info("UPLOAD token dibuat: " + token + " filename=" + filename);
        return token;
    }

    /**
     * Membuat token sekali pakai untuk transfer PUSH (server mengirim berkas ke Agent).
     *
     * @param filename nama berkas yang ada di direktori unggahan
     * @param size     ukuran berkas dalam byte (dari {@link File#length()})
     * @return token unik untuk transfer ini
     */
    public String createPushToken(String filename, long size) {
        String token = UUID.randomUUID().toString();
        tokens.put(token, new TransferToken(filename, size, false));
        log.info("PUSH token dibuat: " + token + " filename=" + filename + " size=" + size);
        return token;
    }

    /**
     * Mengambil hasil akhir transfer sebuah token. Pemanggil (thread perintah) dapat
     * menunggu {@code CompletableFuture} ini sampai transfer selesai atau timeout.
     *
     * @param token token transfer
     * @return future hasil transfer, atau null bila token tidak dikenal
     */
    public CompletableFuture<String> getResult(String token) {
        TransferToken tt = tokens.get(token);
        return tt == null ? null : tt.result;
    }

    /**
     * Menandai token sebagai terpakai saat koneksi data masuk. Token hanya boleh
     * dipakai oleh satu koneksi (sekali pakai).
     *
     * @param token token transfer
     * @return state token, atau null bila token tidak dikenal/sudah terpakai
     */
    private TransferToken consume(String token) {
        TransferToken tt = tokens.get(token);
        if (tt == null || !tt.consumed.compareAndSet(false, true)) {
            return null;
        }
        return tt;
    }

    /** Menghapus token dari daftar transfer aktif. */
    public void removeToken(String token) {
        tokens.remove(token);
    }

    /**
     * Memeriksa apakah berkas yang diminta tersedia di direktori unggahan dan
     * mengembalikan ukurannya.
     *
     * @param filename nama berkas (sudah divalidasi)
     * @return ukuran berkas dalam byte, atau -1 bila tidak ada
     */
    public long getUploadedFileSize(String filename) {
        File f = new File(uploadsDir, filename);
        return f.isFile() ? f.length() : -1L;
    }

    /** Menjalankan layanan kanal data: membuka ServerSocket dan thread accept. */
    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        running = true;
        log.info("Data channel aktif di port " + port);

        cleanupScheduler.scheduleAtFixedRate(this::cleanupExpiredTokens, 60, 60, TimeUnit.SECONDS);

        Thread acceptThread = new Thread(this::acceptLoop, "data-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    /** Loop utama penerimaan koneksi data; setiap koneksi ditangani di thread pool. */
    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                pool.execute(() -> handleConnection(client));
            } catch (IOException e) {
                if (running) {
                    log.error("Data accept gagal: " + e.getMessage());
                }
                break;
            }
        }
    }

    /**
     * Menangani satu koneksi data: membaca token, memverifikasi sekali pakai,
     * lalu mengalihkan ke penanganan UPLOAD atau PUSH.
     *
     * @param socket soket data yang baru diterima
     */
    private void handleConnection(Socket socket) {
        try {
            socket.setSoTimeout(300_000); // 5 menit timeout
            DataInputStream dis = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            String token = dis.readUTF();
            TransferToken tt = consume(token);
            if (tt == null) {
                log.warn("Token data tidak valid atau sudah dipakai: " + token);
                socket.close();
                return;
            }
            if (tt.isUpload) {
                handleUpload(socket, dis, tt);
            } else {
                handlePush(socket, tt);
            }
        } catch (IOException e) {
            log.warn("Data connection error: " + e.getMessage());
            try {
                socket.close();
            } catch (IOException ignored) {
                // abaikan
            }
        }
    }

    /**
     * Menerima data dari Inspector, menulis ke direktori unggahan, dan memverifikasi ukuran.
     * Berkas parsial dihapus bila koneksi putus di tengah transfer atau ukuran tidak cocok.
     *
     * @param socket soket data
     * @param dis    stream pembaca data
     * @param tt     state token transfer
     */
    private void handleUpload(Socket socket, DataInputStream dis, TransferToken tt) {
        File target = new File(uploadsDir, tt.filename);
        try {
            String filename = dis.readUTF();
            long size = dis.readLong();
            if (!Protocol.isSafeFilename(filename)) {
                tt.result.complete("ERR nama_berkas_tidak_valid");
                return;
            }
            long maxSize = Long.getLong("netmonitor.max.file.size", 10_485_760L);
            if (size > maxSize) {
                tt.result.complete("ERR melebihi_batas_ukuran");
                return;
            }

            File uploadDir = new File(uploadsDir);
            if (!uploadDir.exists()) {
                uploadDir.mkdirs();
            }
            target = new File(uploadDir, filename);

            long received = 0L;
            try (FileOutputStream fos = new FileOutputStream(target)) {
                byte[] buffer = new byte[CHUNK_SIZE];
                int read;
                while ((read = dis.read(buffer)) != -1) {
                    fos.write(buffer, 0, read);
                    received += read;
                    if (received > size) {
                        break;
                    }
                }
            } catch (IOException e) {
                target.delete();
                tt.result.complete("ERR disk_penuh: " + e.getMessage());
                return;
            }

            if (received != size) {
                target.delete();
                tt.result.complete("ERR ukuran_tidak_sesuai: expected=" + size + " received=" + received);
                return;
            }
            tt.result.complete("OK");
        } catch (IOException e) {
            target.delete();
            tt.result.complete("ERR transfer_gagal: " + e.getMessage());
        } finally {
            try {
                socket.shutdownOutput();
            } catch (IOException ignored) {
                // koneksi sudah tertutup
            }
            try {
                socket.close();
            } catch (IOException ignored) {
                // abaikan
            }
        }
    }

    /**
     * Mengirim berkas dari direktori unggahan ke Agent: header metadata,
     * potongan 8 KB, lalu {@code shutdownOutput()} tanpa menutup soket perintah.
     *
     * @param socket soket data
     * @param tt     state token transfer
     */
    private void handlePush(Socket socket, TransferToken tt) {
        try {
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            dos.writeUTF(tt.filename);
            dos.writeLong(tt.expectedSize);
            dos.flush();

            File source = new File(uploadsDir, tt.filename);
            if (!source.isFile()) {
                tt.result.complete("ERR file_tidak_ada: " + tt.filename);
                try {
                    socket.shutdownOutput();
                } catch (IOException ignored) {
                    // abaikan
                }
                return;
            }

            try (FileInputStream fis = new FileInputStream(source)) {
                byte[] buffer = new byte[CHUNK_SIZE];
                int read;
                while ((read = fis.read(buffer)) != -1) {
                    dos.write(buffer, 0, read);
                }
            }
            dos.flush();
            socket.shutdownOutput();
            tt.result.complete("OK");
        } catch (IOException e) {
            log.warn("PUSH transfer error: " + e.getMessage());
            try {
                socket.shutdownOutput();
            } catch (IOException ignored) {
                // abaikan
            }
            tt.result.complete("ERR " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
                // abaikan
            }
        }
    }

    /** Membersihkan token yang sudah melewati batas waktu tanpa koneksi data masuk. */
    private void cleanupExpiredTokens() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, TransferToken> entry : tokens.entrySet()) {
            if (now - entry.getValue().startTime > TOKEN_TIMEOUT_MS) {
                log.warn("Token kadaluarsa: " + entry.getKey());
                entry.getValue().result.complete("ERR token_kadaluarsa");
                tokens.remove(entry.getKey());
            }
        }
    }

    /** Menghentikan layanan kanal data dan melepaskan semua sumber daya. */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // abaikan
        }
        cleanupScheduler.shutdownNow();
        pool.shutdownNow();
        for (TransferToken tt : tokens.values()) {
            if (!tt.result.isDone()) {
                tt.result.complete("ERR server_stopped");
            }
        }
        tokens.clear();
    }
}
