package netmonitor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * MESIN JARINGAN klien NetMonitor (Pertemuan 5): koneksi TCP, login, thread penerima di latar belakang,
 * balasan heartbeat, deteksi koneksi putus diam-diam, dan sambung ulang otomatis untuk Agent.
 *
 * Kelas ini sengaja BEBAS dari tampilan: tidak ada System.out/err, Scanner, atau System.exit. Semua
 * kejadian dilaporkan ke {@link ClientListener}. Dengan begitu, tampilan konsol dan GUI web memakai
 * mesin yang sama tanpa mengubahnya, dan soket tidak pernah bercampur dengan kode tampilan.
 *
 * Penanganan kesalahan: setiap kegagalan dilaporkan dengan PENYEBAB sebenarnya (server mati, jaringan
 * putus, DNS gagal, kredensial salah, dll.), bukan pesan umum yang menyesatkan.
 */
public final class ClientEngine {

    private static final int CONNECT_TIMEOUT_MS = 5_000;     // batas waktu menyambung (connect timeout)
    private static final int LOGIN_TIMEOUT_MS = 15_000;      // batas menunggu jawaban LOGIN (server mungkin mencari GeoIP)
    private static final int RESPONSE_TIMEOUT_MS = 25_000;   // batas menunggu jawaban sebuah perintah (read timeout)
    private static final int POLL_MS = 1_000;                // setSoTimeout pendek agar pemantau koneksi bisa berjalan
    private static final int DNS_TIMEOUT_MS = 5_000;
    // Tidak ada sinyal (heartbeat) dari server selama ini -> koneksi dianggap putus.
    private static final long WATCHDOG_MS = Long.getLong("netmonitor.heartbeat.timeout", 30L) * 1000L;
    private static final int RECONNECT_ATTEMPTS = Integer.getInteger("netmonitor.reconnect.attempts", 20);
    private static final long RECONNECT_DELAY_MS = Long.getLong("netmonitor.reconnect.delay", 3000L);
    private static final String LOST = "\u0000LOST";         // penanda internal di antrean jawaban

    private final ClientConfig cfg;
    private final ClientListener ui;
    private volatile boolean quitRequested;
    private volatile Connection current;

    public ClientEngine(ClientConfig cfg, ClientListener ui) {
        this.cfg = cfg;
        this.ui = ui;
    }

    // ------------------------------------------------------------------ validasi & resolusi host

    /** Hasil validasi host: alamat bila berhasil, atau pesan kesalahan. */
    public static final class HostResult {
        public InetAddress address;
        public String error;
    }

    /** Validasi format + resolusi DNS dengan batas waktu, lengkap dengan penyebab kegagalan yang jelas. */
    public static HostResult resolveHost(String host) {
        HostResult r = new HostResult();
        if (!NetUtil.isValidFormat(host)) {
            r.error = "Format host tidak valid: '" + host + "'. Contoh yang benar: localhost, 192.168.1.5, atau nama.domain";
            return r;
        }
        try {
            r.address = NetUtil.resolveWithTimeout(host, DNS_TIMEOUT_MS);
        } catch (NetUtil.DnsTimeoutException e) {
            r.error = "DNS tidak merespons dalam " + (DNS_TIMEOUT_MS / 1000) + " detik untuk '" + host
                    + "'. Periksa koneksi jaringan/internet, atau gunakan alamat IP server.";
        } catch (UnknownHostException e) {
            r.error = NetUtil.isInternetAvailable()
                    ? "Host '" + host + "' tidak ditemukan. Periksa penulisan nama host."
                    : "Host '" + host + "' tidak dapat diresolusi karena tidak ada koneksi internet/DNS tidak tersedia. "
                      + "Periksa koneksi Anda, atau gunakan alamat IP server (mis. 192.168.1.5).";
        } catch (IOException e) {
            r.error = "Gagal meresolusi host '" + host + "': " + e.getMessage();
        }
        return r;
    }

    /** Validasi host server dan uji jangkauan (hanya peringatan). @return alamat, atau null jika host tidak dapat dipakai. */
    private InetAddress prepare() {
        HostResult hr = resolveHost(cfg.getHost());
        if (hr.error != null) {
            ui.onError(hr.error);
            return null;
        }
        InetAddress address = hr.address;
        ui.onInfo("Server " + cfg.getHost() + " -> " + address.getHostAddress());
        try {
            if (!address.isReachable(2000)) {
                ui.onInfo("Peringatan: host tidak membalas uji isReachable (bisa karena firewall), tetap mencoba menyambung...");
            }
        } catch (IOException e) {
            ui.onInfo("Peringatan: uji isReachable gagal (" + describeIo(e) + ")");
        }
        return address;
    }

    // ------------------------------------------------------------------ API untuk lapisan tampilan

    /** Hasil sebuah permintaan perintah (mode Inspector). */
    public static final class Result {
        public enum Status { OK, TIMEOUT, LOST }

        public final Status status;
        public final String line;
        public final boolean quitting;

        private Result(Status status, String line, boolean quitting) {
            this.status = status;
            this.line = line;
            this.quitting = quitting;
        }
    }

    /**
     * Mode INSPECTOR: sambung dan login satu kali. Setelah berhasil, thread penerima di latar belakang
     * berjalan dan perintah dikirim dengan {@link #request(String)}.
     *
     * @return false bila gagal (alasannya sudah dilaporkan lewat {@link ClientListener#onError})
     */
    public boolean connect() {
        InetAddress address = prepare();
        if (address == null) {
            return false;
        }
        try {
            Connection c = open(address);
            ui.onConnected(c.loginResponse, false);
            current = c;
            c.startReader(null, reason -> ui.onConnectionLost(reason, false, c.serverNotice != null));
            return true;
        } catch (ClientError e) {
            ui.onError(e.getMessage());
            return false;
        }
    }

    /** Mengirim satu perintah dan menunggu jawabannya (paling lama RESPONSE_TIMEOUT_MS). */
    public Result request(String command) {
        Connection c = current;
        if (c == null || c.lost) {
            return new Result(Result.Status.LOST, null, false);
        }
        String first = command.trim().split("\\s+")[0].toUpperCase();
        boolean quitting = first.equals("QUIT") || first.equals("EXIT");
        if (quitting) {
            c.userQuit = true;
        }
        c.awaiting = true;
        try {
            c.send(command);
        } catch (IOException e) {
            c.awaiting = false;
            c.markLost(describeIo(e));
            return new Result(Result.Status.LOST, null, quitting);
        }
        String resp;
        try {
            resp = c.responses.poll(RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(Result.Status.LOST, null, quitting);
        } finally {
            c.awaiting = false;
        }
        if (resp == null) {
            return new Result(Result.Status.TIMEOUT, null, quitting);
        }
        if (resp.equals(LOST)) {
            return new Result(Result.Status.LOST, null, quitting);
        }
        return new Result(Result.Status.OK, resp, quitting);
    }

    /**
     * Mode AGENT: tetap terhubung, menjawab permintaan server, dan menyambung ulang otomatis bila terputus.
     * Berjalan sampai pengguna meminta berhenti atau sambung ulang gagal.
     *
     * @return kode keluar: 0 = berhenti normal, 1 = gagal
     */
    public int runAgent() {
        InetAddress address = prepare();
        if (address == null) {
            return 1;
        }
        boolean everConnected = false;
        int attempt = 0;
        while (true) {
            Connection c = null;
            try {
                c = open(address);
                ui.onConnected(c.loginResponse, everConnected);
                everConnected = true;
                attempt = 0;
                current = c;
                final Connection conn = c;
                c.startReader(line -> answerRequest(conn, line), reason -> { /* ditangani oleh loop ini */ });
                try {
                    c.lostLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (quitRequested) {
                    return 0;
                }
                ui.onConnectionLost(c.lostReason, true, c.serverNotice != null);
            } catch (ClientError e) {
                ui.onError(e.getMessage());
                if (!everConnected || !e.retryable) {
                    return 1;
                }
            } finally {
                current = null;
                if (c != null) {
                    c.close();
                }
            }
            attempt++;
            if (attempt > RECONNECT_ATTEMPTS || quitRequested) {
                if (!quitRequested) {
                    ui.onError("Gagal menyambung ulang setelah " + RECONNECT_ATTEMPTS
                            + " percobaan. Periksa server dan jaringan, lalu jalankan ulang Agent.");
                    return 1;
                }
                return 0;
            }
            ui.onReconnecting(attempt, RECONNECT_ATTEMPTS, RECONNECT_DELAY_MS);
            long until = System.currentTimeMillis() + RECONNECT_DELAY_MS;
            while (System.currentTimeMillis() < until && !quitRequested) {
                sleep(100);
            }
            if (quitRequested) {
                return 0;
            }
        }
    }

    /** Pengguna meminta berhenti (Agent): kirim QUIT bila masih terhubung dan hentikan sambung ulang. */
    public void requestQuit() {
        quitRequested = true;
        Connection c = current;
        if (c != null && !c.lost) {
            c.userQuit = true;
            try {
                c.send("QUIT");
            } catch (IOException ignored) {
                // koneksi sudah putus
            }
        }
    }

    /** Menutup koneksi saat ini dengan rapi (shutdownOutput lalu close). */
    public void close() {
        Connection c = current;
        if (c != null) {
            c.close();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ koneksi & login

    /** Kegagalan yang sudah dijelaskan dengan bahasa yang mudah dipahami. */
    private static final class ClientError extends Exception {
        private static final long serialVersionUID = 1L;
        final boolean retryable;

        ClientError(String message, boolean retryable) {
            super(message);
            this.retryable = retryable;
        }
    }

    /** Membuka soket (dengan connect timeout), lalu login. */
    private Connection open(InetAddress address) throws ClientError {
        String host = cfg.getHost();
        int port = cfg.getPort();
        Socket socket = new Socket();
        try {
            try {
                socket.connect(new InetSocketAddress(address, port), CONNECT_TIMEOUT_MS);
            } catch (SocketTimeoutException e) {
                throw new ClientError("Waktu habis (" + (CONNECT_TIMEOUT_MS / 1000) + " detik) saat menyambung ke "
                        + host + ":" + port + ". Periksa alamat IP, apakah kedua perangkat berada di jaringan yang sama, "
                        + "dan firewall di laptop server.", true);
            } catch (ConnectException e) {
                throw mapConnectError(e);
            } catch (NoRouteToHostException e) {
                throw new ClientError("Tidak ada rute ke " + host + ". Periksa bahwa kedua perangkat berada di jaringan "
                        + "yang sama (Wi-Fi/hotspot) dan alamat IP server benar.", true);
            } catch (IOException e) {
                throw new ClientError("Tidak dapat menyambung ke " + host + ":" + port + ": " + describeIo(e) + ".", true);
            }
            socket.setSoTimeout(POLL_MS);
            socket.setKeepAlive(true);
            Connection c = new Connection(socket);
            try {
                c.send("LOGIN " + cfg.getRole() + " " + cfg.getUsername() + " " + cfg.getPassword());
                c.loginResponse = c.readLoginResponse();
            } catch (IOException e) {
                throw new ClientError("Koneksi terputus saat login: " + describeIo(e) + ".", true);
            }
            if (!c.loginResponse.startsWith("200")) {
                throw explainLoginFailure(c.loginResponse);
            }
            return c;
        } catch (ClientError e) {
            closeQuietly(socket);
            throw e;
        } catch (IOException e) {
            closeQuietly(socket);
            throw new ClientError("Kesalahan jaringan: " + describeIo(e) + ".", true);
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // abaikan
        }
    }

    private ClientError mapConnectError(ConnectException e) {
        String host = cfg.getHost();
        int port = cfg.getPort();
        String m = String.valueOf(e.getMessage()).toLowerCase();
        if (m.contains("refused")) {
            return new ClientError("Server menolak koneksi di " + host + ":" + port + ". Pastikan server NetMonitor sudah "
                    + "menyala dan nomor port benar.", true);
        }
        if (m.contains("timed out")) {
            return new ClientError("Waktu habis saat menyambung ke " + host + ":" + port + ". Periksa alamat IP, jaringan, "
                    + "dan firewall di laptop server.", true);
        }
        if (m.contains("unreachable") || m.contains("network is down")) {
            return new ClientError("Jaringan tidak tersedia (Wi-Fi/Ethernet terputus) sehingga " + host
                    + " tidak dapat dijangkau.", true);
        }
        return new ClientError("Tidak dapat menyambung ke " + host + ":" + port + ": " + e.getMessage() + ".", true);
    }

    /** Menjelaskan penolakan login berdasarkan KODE respons server, bukan menebak. */
    private ClientError explainLoginFailure(String resp) {
        if (!resp.matches("^\\d{3}( .*)?$")) {
            return new ClientError("Terhubung, tetapi respons bukan dari server NetMonitor (diterima: '"
                    + Protocol.clean(resp, 60) + "'). Periksa host dan port: mungkin itu server lain.", false);
        }
        String code = resp.substring(0, 3);
        String text = resp.length() > 4 ? resp.substring(4) : "";
        switch (code) {
            case "401":
                return new ClientError("Login ditolak (" + resp + "). Username atau password tidak cocok dengan "
                        + "akun yang terdaftar di server.", false);
            case "400":
                if (text.contains("Username Sudah Ada")) {
                    return new ClientError("Login ditolak: username '" + cfg.getUsername() + "' sedang dipakai oleh sesi lain. Tutup "
                            + "sesi itu atau pakai akun lain. Jika sesi lama sebenarnya sudah putus, server membersihkannya "
                            + "dalam beberapa detik dan Anda dapat mencoba lagi.", true);
                }
                return new ClientError("Permintaan login tidak valid (" + resp + ").", false);
            case "403":
                return new ClientError("Akses ditolak (" + resp + ").", false);
            case "408":
                return new ClientError("Server menutup sesi karena waktu habis (" + resp + ").", true);
            case "500":
                return new ClientError("Server mengalami kesalahan internal (" + resp + "). Coba lagi; jika berulang, "
                        + "periksa log server.", true);
            case "503":
                return new ClientError("Server tidak dapat menerima koneksi (" + resp + ").", true);
            default:
                return new ClientError("Respons login tidak dikenali dari server (" + resp + ").", false);
        }
    }

    /** Mengubah exception jaringan menjadi kalimat penyebab yang mudah dibaca. */
    static String describeIo(IOException e) {
        String m = String.valueOf(e.getMessage()).toLowerCase();
        if (e instanceof SocketTimeoutException) {
            return "waktu tunggu habis (tidak ada jawaban)";
        }
        if (e instanceof NoRouteToHostException) {
            return "tidak ada rute ke host (periksa jaringan)";
        }
        if (m.contains("connection reset") || m.contains("forcibly closed")) {
            return "koneksi direset oleh server atau jaringan (server mati mendadak atau jaringan putus)";
        }
        if (m.contains("broken pipe") || m.contains("connection abort") || m.contains("software caused")) {
            return "koneksi terputus saat mengirim data (server atau jaringan terputus)";
        }
        if (m.contains("network is unreachable") || m.contains("network is down")) {
            return "jaringan tidak tersedia (Wi-Fi/Ethernet terputus)";
        }
        if (m.contains("socket closed")) {
            return "koneksi ditutup";
        }
        return "gangguan jaringan (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")";
    }

    // ------------------------------------------------------------------ satu koneksi + thread penerima

    /**
     * Satu koneksi ke server. Thread penerima (daemon) membaca semua baris dari server: membalas heartbeat,
     * meneruskan permintaan server ke Agent, mengantre jawaban perintah, dan memantau apakah server masih
     * "terdengar" (watchdog).
     */
    private final class Connection {
        final Socket socket;
        final Protocol.LineReader reader;
        final BufferedWriter writer;
        final BlockingQueue<String> responses = new LinkedBlockingQueue<>();
        final CountDownLatch lostLatch = new CountDownLatch(1);
        final AtomicBoolean marked = new AtomicBoolean();   // markLost() hanya diproses sekali
        volatile long lastSeen = System.currentTimeMillis();
        volatile boolean lost;
        volatile boolean awaiting;      // sedang menunggu jawaban sebuah perintah?
        volatile boolean userQuit;      // pengguna sendiri yang mengakhiri sesi
        volatile String lostReason = "";
        volatile String serverNotice;   // pesan resmi dari server sebelum koneksi ditutup
        String loginResponse = "";
        private Consumer<String> onRequest;
        private Consumer<String> onLost;

        Connection(Socket socket) throws IOException {
            this.socket = socket;
            this.reader = new Protocol.LineReader(
                    new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)));
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        }

        synchronized void send(String line) throws IOException {
            Protocol.writeLine(writer, line);
        }

        /** Menunggu jawaban LOGIN sampai LOGIN_TIMEOUT_MS. */
        String readLoginResponse() throws IOException, ClientError {
            long deadline = System.currentTimeMillis() + LOGIN_TIMEOUT_MS;
            while (true) {
                try {
                    String line = reader.readLine();
                    if (line == null) {
                        throw new ClientError("Server menutup koneksi sebelum login selesai (server mati atau jaringan "
                                + "putus).", true);
                    }
                    lastSeen = System.currentTimeMillis();
                    return line;
                } catch (SocketTimeoutException e) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new ClientError("Server tidak menjawab login dalam " + (LOGIN_TIMEOUT_MS / 1000)
                                + " detik. Server sibuk, jaringan terputus, atau itu bukan server NetMonitor.", true);
                    }
                } catch (Protocol.LineTooLongException e) {
                    throw new ClientError("Respons dari server tidak valid (terlalu panjang). Itu kemungkinan bukan server "
                            + "NetMonitor; periksa host dan port.", false);
                }
            }
        }

        void startReader(Consumer<String> onRequest, Consumer<String> onLost) {
            this.onRequest = onRequest;
            this.onLost = onLost;
            Thread t = new Thread(this::readLoop, "pembaca-server");
            t.setDaemon(true);
            t.start();
        }

        private void readLoop() {
            while (!lost) {
                String line;
                try {
                    line = reader.readLine();
                } catch (SocketTimeoutException e) {
                    long silent = System.currentTimeMillis() - lastSeen;
                    if (silent > WATCHDOG_MS) {
                        markLost("tidak ada sinyal dari server selama " + (silent / 1000)
                                + " detik (jaringan putus, server mati, atau laptop server tertidur)");
                        return;
                    }
                    continue;
                } catch (Protocol.LineTooLongException e) {
                    markLost("server mengirim data yang tidak valid");
                    return;
                } catch (IOException e) {
                    markLost(describeIo(e));
                    return;
                }
                if (line == null) {
                    markLost("server menutup koneksi (server mati mendadak atau jaringan putus)");
                    return;
                }
                lastSeen = System.currentTimeMillis();
                if (line.equals(Protocol.HEARTBEAT)) {
                    try {
                        send(Protocol.HEARTBEAT_ACK);
                    } catch (IOException e) {
                        markLost(describeIo(e));
                        return;
                    }
                    continue;
                }
                if (line.startsWith("REQ ")) {
                    if (onRequest != null) {
                        onRequest.accept(line);
                    }
                    continue;
                }
                if (line.startsWith("503 ERR Server Dimatikan")) {
                    serverNotice = "server dimatikan oleh administrator";
                } else if (line.startsWith("408 ERR Sesi Timeout")) {
                    serverNotice = "sesi ditutup server karena tidak aktif/waktu habis (" + line.substring(4) + ")";
                }
                if (awaiting) {
                    responses.offer(line);
                } else {
                    ui.onServerMessage(line);
                }
            }
        }

        void markLost(String reason) {
            if (!marked.compareAndSet(false, true)) {
                return;
            }
            lostReason = serverNotice != null ? serverNotice : reason;
            // Pesan ke pengguna dilaporkan LEBIH DULU, baru thread yang menunggu dibangunkan; kalau terbalik,
            // program bisa selesai sebelum pesannya sempat ditampilkan.
            if (!userQuit && onLost != null) {
                onLost.accept(lostReason);
            }
            lost = true;
            responses.offer(LOST);
            lostLatch.countDown();
        }

        void close() {
            try {
                socket.shutdownOutput();   // tutup sisi kirim dengan rapi sebelum soket ditutup
            } catch (IOException ignored) {
                // sudah tertutup
            }
            closeQuietly(socket);
        }
    }

    // ------------------------------------------------------------------ mode AGENT

    /** Menjawab permintaan server: "REQ <id> SYSINFO" dibalas "RES <id> <data>". */
    private void answerRequest(Connection c, String line) {
        String[] p = line.split("\\s+");
        String id = p.length > 1 ? p[1] : "0";
        try {
            if (p.length >= 3 && p[2].equals("SYSINFO")) {
                c.send("RES " + id + " " + collectSysinfo());
            } else {
                c.send("RES " + id + " ERR perintah_tidak_dikenal");
            }
        } catch (IOException e) {
            c.markLost(describeIo(e));
        }
    }

    @SuppressWarnings("deprecation")
    private static String collectSysinfo() {
        String os = System.getProperty("os.name", "unknown").replace(' ', '_');
        int cores = Runtime.getRuntime().availableProcessors();
        long totalBytes;
        try {
            totalBytes = ((com.sun.management.OperatingSystemMXBean)
                    ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
        } catch (RuntimeException | LinkageError e) {
            totalBytes = Runtime.getRuntime().maxMemory();
        }
        long ramGb = Math.max(1, Math.round(totalBytes / (1024.0 * 1024 * 1024)));
        long uptimeSec = ManagementFactory.getRuntimeMXBean().getUptime() / 1000;
        return "os=" + os + " cores=" + cores + " ram=" + ramGb + "GB agentUptime=" + uptimeSec + "s";
    }
}
