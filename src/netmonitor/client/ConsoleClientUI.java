package netmonitor.client;

import java.util.Scanner;
import netmonitor.common.Protocol;

/**
 * TAMPILAN KONSOL (CLI) klien NetMonitor. Seluruh pembacaan keyboard dan pencetakan ke layar ada di sini;
 * jaringan ditangani oleh {@link ClientEngine}. GUI nanti cukup menjadi {@link ClientListener} lain.
 */
public final class ConsoleClientUI implements ClientListener {

    private static final Scanner STDIN = new Scanner(System.in);

    private final boolean agent;
    private ClientEngine engine;
    private static final boolean ANSI = !"false".equalsIgnoreCase(System.getProperty("netmonitor.cli.ansi", "true"));
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RED = "\u001B[31m";

    /**
     * Mengembalikan teks dengan kode pewarnaan ANSI bila aktif, atau teks asli bila dinonaktifkan
     * melalui properti -Dnetmonitor.cli.ansi=false.
     *
     * @param s  teks yang akan diberi warna
     * @param code kode warna ANSI (mis. "\u001B[32m" untuk hijau)
     * @return teks dengan pemutih/warna, atau teks polos bila ANSI dimatikan
     */
    private static String ansiOn(String s, String code) {
        return ANSI ? code + s + ANSI_RESET : s;
    }

    /**
     * Membuat tampilan konsol.
     *
     * @param agent true bila klien berperan AGENT
     */
    private ConsoleClientUI(boolean agent) {
        this.agent = agent;
    }

    // ------------------------------------------------------------------ pengaturan dari argumen atau tanya-jawab

    /**
     * Membaca pengaturan dari argumen baris perintah, atau menanyakannya satu per satu (dengan validasi).
     *
     * @param args argumen baris perintah: [host port peran username password]
     * @return konfigurasi klien yang sudah tervalidasi
     */
    public static ClientConfig promptConfig(String[] args) {
        // Jika menggunakan argumen baris perintah (CLI args)
        if (args.length >= 5) {
            int port = parsePort(args[1], true); // true = matikan program jika salah
            String role = args[2].toUpperCase();
            if (!Protocol.isValidRole(role)) {
                fail("Peran harus AGENT atau INSPECTOR (diketik: '" + role + "').");
            }
            return new ClientConfig(args[0], port, role, args[3], args[4]);
        }

        String host;
        int port = Protocol.DEFAULT_PORT;
        String role;

        // 1. Input HOST (Looping sampai valid dan bisa diresolusi)
        while (true) {
            String h = readInput("Host server [localhost]: ");
            String tempHost = h.isEmpty() ? "localhost" : h;
            ClientEngine.HostResult hr = ClientEngine.resolveHost(tempHost);
            if (hr.error == null) {
                host = tempHost;
                break;
            }
            System.out.println("  -> Error: " + hr.error);
        }

        // 2. Input PORT (Looping sampai valid)
        while (true) {
            String pt = readInput("Port [" + Protocol.DEFAULT_PORT + "]: ");
            if (pt.isEmpty()) {
                break; // Gunakan port default
            }
            int parsed = parsePort(pt, false); // false = jangan matikan program, kembalikan -1
            if (parsed != -1) {
                port = parsed;
                break;
            }
            System.out.println("  -> Error: Port tidak valid. Harap masukkan angka (1-65535).");
        }

        // 3. Input ROLE (Looping sampai valid)
        while (true) {
            role = readInput("Peran (AGENT/INSPECTOR): ").toUpperCase();
            if (Protocol.isValidRole(role)) {
                break;
            }
            System.out.println("  -> Error: Peran tidak dikenali. Ketik AGENT atau INSPECTOR.");
        }

        // 4. Input Username & Password
        String user = readInput("Username: ");
        String pass = readInput("Password: ");
        return new ClientConfig(host, port, role, user, pass);
    }

    /**
     * Membaca satu baris input dari keyboard; jika input ditutup (EOF) program berhenti dengan pesan.
     *
     * @param prompt teks prompt yang ditampilkan
     * @return input pengguna yang telah dibersihkan spasi di tepi
     */
    private static String readInput(String prompt) {
        System.out.print(prompt);
        if (!STDIN.hasNextLine()) {
            System.out.println();
            fail("Input ditutup (tidak ada data lagi dari keyboard/pipe). Program dihentikan.");
            return "";
        }
        return STDIN.nextLine().trim();
    }

    /**
     * Mem-parsing string menjadi port.
     * @param s string yang akan diparsing
     * @param fatal Jika true, aplikasi berhenti (System.exit) jika format salah.
     *              Jika false, mengembalikan -1 untuk ditangani oleh pemanggil.
     * @return port yang valid, atau -1 bila input tidak valid dan fatal = false
     */
    private static int parsePort(String s, boolean fatal) {
        try {
            int p = Integer.parseInt(s.trim());
            if (p >= 1 && p <= 65535) {
                return p;
            }
        } catch (NumberFormatException ignored) {
            // Abaikan, akan ditangani di bawah
        }
        if (fatal) {
            fail("Port tidak valid: '" + s + "'. Gunakan angka 1-65535.");
        }
        return -1;
    }

    /**
     * Mencetak pesan ke stderr dan menghentikan program.
     *
     * @param message pesan kesalahan
     */
    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }

    // ------------------------------------------------------------------ alur program

    /**
     * Menjalankan klien sesuai peran.
     *
     * @param cfg konfigurasi klien
     * @return kode keluar proses
     */
    public static int run(ClientConfig cfg) {
        ConsoleClientUI ui = new ConsoleClientUI(cfg.isAgent());
        ui.engine = new ClientEngine(cfg, ui);
        return cfg.isAgent() ? ui.runAgent() : ui.runInspector();
    }

    /**
     * Menjalankan mode Agent: membaca ketikan 'quit'/'exit' lalu menjalankan mesin Agent.
     *
     * @return kode keluar dari mesin Agent
     */
    private int runAgent() {
        // Thread kecil membaca ketikan pengguna selama Agent hidup (termasuk saat sambung ulang).
        // Agent diizinkan mengirim MSG dan QUIT; perintah administratif akan ditolak server (403).
        Thread console = new Thread(() -> {
            while (STDIN.hasNextLine()) {
                String in = STDIN.nextLine().trim();
                if (in.isEmpty()) {
                    continue;
                }
                if (in.equalsIgnoreCase("quit") || in.equalsIgnoreCase("exit")) {
                    engine.requestQuit();
                    return;
                }
                try {
                    ClientEngine.Result r = engine.request(in);
                    if (r.line != null) {
                        System.out.println(r.line);
                        if (r.quitting) {
                            engine.close();
                            return;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Gagal mengirim perintah: " + e.getMessage());
                }
            }
        }, "agent-stdin");
        console.setDaemon(true);
        console.start();
        return engine.runAgent();
    }

    /**
     * Menjalankan mode Inspector: menyambung ke server lalu membaca perintah dari pengguna.
     *
     * @return kode keluar proses
     */
    private int runInspector() {
        if (!engine.connect()) {
            return 1;
        }
        System.out.println("Perintah: LIST | SYSINFO <agentId> | PING <host> | RESOLVE <domain> | BROADCAST <teks> | MSG <username> <teks> | EXEC <agentId> <namaPerintah> | UPLOAD <pathLokal> | PUSH <agentId> <namaBerkas> | SUBSCRIBE <udpPort> | UNSUBSCRIBE | WATCH | QUIT");
        // Pemisah visual dan ringkasan opsi menu bernomor saat klien siap menerima perintah.
        System.out.println(ansiOn("=========================================", ANSI_CYAN));
        System.out.println(ansiOn("NetMonitor Inspector - SIAP", ANSI_BOLD + ANSI_CYAN));
        System.out.println(ansiOn("=========================================", ANSI_CYAN));
        System.out.println("  # 1 LIST");
        System.out.println("  # 2 SYSINFO <agentId>");
        System.out.println("  # 3 PING <host>");
        System.out.println("  # 4 RESOLVE <domain>");
        System.out.println("  # 5 BROADCAST <teks>");
        System.out.println("  # 6 MSG <username> <teks>");
        System.out.println("  # 7 EXEC <agentId> <namaPerintah>");
        System.out.println("  # 8 UPLOAD <pathLokal>");
        System.out.println("  # 9 PUSH <agentId> <namaBerkas>");
        System.out.println("  # 10 SUBSCRIBE <udpPort>");
        System.out.println("  # 11 UNSUBSCRIBE");
        System.out.println("  # 12 WATCH");
        System.out.println("  # 13 QUIT / EXIT");
        System.out.println("  # 14 HELP");
        System.out.println(ansiOn("=========================================", ANSI_CYAN));
        while (true) {
            System.out.print("netmonitor> ");
            String cmd = STDIN.hasNextLine() ? STDIN.nextLine().trim() : "QUIT";
            if (cmd.isEmpty()) {
                continue;
            }
            if (cmd.equalsIgnoreCase("HELP")) {
                showHelp();
                continue;
            }
            String first = cmd.trim().split("\\s+")[0].toUpperCase();
            if ("UPLOAD".equals(first)) {
                ClientEngine.Result rup = engine.requestUpload(cmd);
                if (rup.line != null) {
                    System.out.println(rup.line);
                }
                continue;
            }
            ClientEngine.Result r = engine.request(cmd);
            switch (r.status) {
                case OK:
                    System.out.println(r.line);
                    printHint(r.line);
                    // Buka socket UDP lokal setelah SUBSCRIBE berhasil (logika soket di ClientEngine, bukan UI).
                    if (first.equals("SUBSCRIBE")) {
                        String[] toks = cmd.trim().split("\\s+");
                        if (toks.length == 2) {
                            try {
                                int port = Integer.parseInt(toks[1]);
                                if (engine.subscribeTelemetry(port)) {
                                    System.out.println("  Telemetri UDP aktif di port " + port + ".");
                                }
                            } catch (NumberFormatException ignored) {
                                // port tidak valid; server sudah menolak
                            }
                        }
                    } else if (first.equals("UNSUBSCRIBE")) {
                        engine.unsubscribeTelemetry();
                    }
                    if (r.quitting) {
                        engine.close();
                        return 0;
                    }
                    break;
                case TIMEOUT:
                    System.out.println("Server tidak menjawab perintah dalam 25 detik "
                            + "(server sibuk atau macet, walau koneksi masih terlihat hidup). Sesi diakhiri; coba lagi.");
                    engine.close();
                    return 0;
                default:    // LOST: pesannya sudah ditampilkan oleh onConnectionLost
                    engine.close();
                    return 0;
            }
        }
    }

    /** Satu baris keterangan untuk respons gagal, agar pengguna tahu apa artinya dan apa yang harus dilakukan. */
    private static void printHint(String resp) {
        if (resp.length() < 3 || resp.startsWith("200")) {
            return;
        }
        String hint;
        switch (resp.substring(0, 3)) {
            case "400": hint = "Perintah/argumen salah atau tidak lengkap. Periksa format perintah."; break;
            case "401": hint = "Belum login atau kredensial salah."; break;
            case "403":
                // Perbedaan penyebab 403: biar pengguna pikir ANEH mesin tersebut
                // terjal Rank sampai server menjawab.
                String m = resp.toLowerCase();
                hint = m.contains("whitelist")
                        ? "Perintah ini tidak ada dalam daftar perintah yang diizinkan (whitelist); tidak dapat dijalankan."
                        : "Perintah ini hanya boleh dipakai peran INSPECTOR.";
                break;
            case "404": hint = "Yang diminta tidak ditemukan (agent tidak terhubung atau domain tidak dikenal)."; break;
            case "408": hint = "Waktu habis: target atau layanan tidak menjawab tepat waktu."; break;
            case "500": hint = "Terjadi kesalahan di server atau di Agent."; break;
            case "502": hint = "Agent terputus saat diminta."; break;
            case "503": hint = "Layanan atau jaringan di sisi server sedang tidak tersedia."; break;
            default: return;
        }
        System.out.println("  Keterangan: " + hint);
    }

    // ------------------------------------------------------------------ kejadian dari mesin jaringan

    /** @param message informasi biasa yang akan dicetak ke konsol. */
    @Override
    public void onInfo(String message) {
        System.out.println(ansiOn(message, ANSI_CYAN));
    }

    /** @param message galat atau peringatan yang akan dicetak ke stderr. */
    @Override
    public void onError(String message) {
        System.err.println(ansiOn(message, ANSI_RED));
    }

    /**
     * Mencetak respons login ke konsol.
     *
     * @param loginResponse isi respons server
     * @param reconnected   true bila ini hasil sambung ulang otomatis
     */
    @Override
    public void onConnected(String loginResponse, boolean reconnected) {
        System.out.println("Server: " + loginResponse);
        if (reconnected) {
            System.out.println("Tersambung kembali ke server.");
        }
        if (agent) {
            System.out.println(ansiOn("Agent aktif. Ketik 'quit' atau 'exit' lalu Enter untuk keluar.", ANSI_GREEN));
            System.out.println(ansiOn("Perintah Agent: MSG <username> <teks> | QUIT", ANSI_CYAN));
        }
    }

    /** @param line baris dari server yang bukan jawaban perintah. */
    @Override
    public void onServerMessage(String line) {
        System.out.println("[Server] " + line);
    }

    /** @param line pesan chat masuk dari server (BCAST / MSG). */
    @Override
    public void onChatMessage(String line) {
        // Pesan chat masuk (BCAST / MSG): tampilkan di bawah prompt tanpa menghapus input pengguna.
        System.out.println("[Chat] " + line);
    }

    /**
     * Mencetak ringkasan telemetri UDP dari server.
     *
     * @param agentId    identitas Agent
     * @param cpuLoad    beban CPU (0.0 - 1.0, -1.0 bila tidak tersedia)
     * @param memPercent persentase penggunaan memori (0 - 100)
     * @param seqNum     nomor urut paket
     */
    @Override
    public void onTelemetry(String agentId, double cpuLoad, double memPercent, int seqNum) {
        // Telemetri UDP dari server: ringkasan singkat di konsol Inspector.
        System.out.println(String.format(java.util.Locale.US,
                "[Telemetry] %s: CPU %.2f%% | Mem %.2f%% (seq %d)",
                agentId, cpuLoad * 100.0, memPercent, seqNum));
    }

    /**
     * Menampilkan progres transfer berkas biner di konsol Inspector/Agent.
     *
     * @param direction      "UPLOAD" atau "PUSH"
     * @param bytesTransferred jumlah byte yang sudah diproses
     * @param totalExpected  total byte yang diharapkan (0 bila tidak diketahui)
     */
    @Override
    public void onTransferProgress(String direction, long bytesTransferred, long totalExpected) {
        double percent = totalExpected > 0 ? (double) bytesTransferred / totalExpected * 100.0 : 0.0;
        System.out.printf("\r[Transfer %s] %d/%d byte (%.1f%%)",
                direction, bytesTransferred, totalExpected, percent);
        if (percent >= 100.0) {
            System.out.println();
        }
    }

    /**
     * Menangani koneksi terputus: tampilkan pesan dan hentikan program.
     *
     * @param reason        penyebab yang sudah berupa kalimat mudah dibaca
     * @param willReconnect true bila mesin akan mencoba menyambung ulang
     * @param byServer      true bila server sendiri yang memberi tahu
     */
    @Override
    public void onConnectionLost(String reason, boolean willReconnect, boolean byServer) {
        if (willReconnect) {
            System.out.println(ansiOn("[KONEKSI TERPUTUS] Agent kehilangan koneksi ke server: " + reason + ".", ANSI_RED));
            return;
        }
        // Inspector: tidak menyambung ulang; tampilkan penyebab lalu hentikan program.
        System.out.println();
        System.out.println(ansiOn("[KONEKSI TERPUTUS] Koneksi ke server terputus: " + reason + ".", ANSI_RED));
        System.out.println(byServer
                ? "Jalankan ulang Inspector untuk login kembali."
                : "Jalankan ulang Inspector setelah server dan jaringan pulih.");
        System.exit(2);
    }

    /**
     * Menampilkan pemberitahuan percobaan sambung ulang.
     *
     * @param attempt    nomor percobaan
     * @param maxAttempts jumlah maksimal percobaan
     * @param delayMs     jeda sebelum percobaan berikutnya (ms)
     */
    @Override
    public void onReconnecting(int attempt, int maxAttempts, long delayMs) {
        System.out.println("Mencoba menyambung ulang dalam " + (delayMs / 1000.0) + " detik (percobaan "
                + attempt + "/" + maxAttempts + "). Ketik 'quit' atau 'exit' untuk berhenti.");
    }

    /** Menampilkan daftar perintah yang tersedia. */
    public void showHelp() {
        System.out.println("\nPerintah yang tersedia:");
        System.out.println("  LOGIN <role> <user> <pass>");
        System.out.println("  LIST");
        System.out.println("  SYSINFO <agentId>");
        System.out.println("  PING <host>");
        System.out.println("  RESOLVE <domain>");
        System.out.println("  BROADCAST <teks>");
        System.out.println("  MSG <username> <teks>");
        System.out.println("  EXEC <agentId> <namaPerintah>");
        System.out.println("  UPLOAD <pathLokal>     (unggah berkas patch ke server)");
        System.out.println("  PUSH <agentId> <namaBerkas> (kirim patch ke Agent)");
        System.out.println("  SUBSCRIBE <udpPort>   (mulai terima telemetri UDP)");
        System.out.println("  UNSUBSCRIBE           (berhenti terima telemetri UDP)");
        System.out.println("  WATCH                 (metrik terakhir semua Agent)");
        System.out.println("  QUIT / EXIT");
    }
}
