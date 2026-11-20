package netmonitor;

import java.util.Scanner;

/**
 * TAMPILAN KONSOL (CLI) klien NetMonitor. Seluruh pembacaan keyboard dan pencetakan ke layar ada di sini;
 * jaringan ditangani oleh {@link ClientEngine}. GUI nanti cukup menjadi {@link ClientListener} lain.
 */
public final class ConsoleClientUI implements ClientListener {

    private static final Scanner STDIN = new Scanner(System.in);

    private final boolean agent;
    private ClientEngine engine;

    private ConsoleClientUI(boolean agent) {
        this.agent = agent;
    }

    // ------------------------------------------------------------------ pengaturan dari argumen atau tanya-jawab

    /** Membaca pengaturan dari argumen baris perintah, atau menanyakannya satu per satu (dengan validasi). */
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

    /** Membaca satu baris input; jika input ditutup (EOF) program berhenti dengan pesan, bukan crash. */
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
     * @param fatal Jika true, aplikasi berhenti (System.exit) jika format salah.
     *              Jika false, mengembalikan -1 untuk ditangani oleh pemanggil.
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

    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }

    // ------------------------------------------------------------------ alur program

    /** Menjalankan klien sesuai peran. @return kode keluar proses. */
    public static int run(ClientConfig cfg) {
        ConsoleClientUI ui = new ConsoleClientUI(cfg.isAgent());
        ui.engine = new ClientEngine(cfg, ui);
        return cfg.isAgent() ? ui.runAgent() : ui.runInspector();
    }

    private int runAgent() {
        // Thread kecil membaca ketikan 'quit'/'exit' selama Agent hidup (termasuk saat sambung ulang).
        Thread console = new Thread(() -> {
            while (STDIN.hasNextLine()) {
                String in = STDIN.nextLine().trim();
                if (in.equalsIgnoreCase("quit") || in.equalsIgnoreCase("exit")) {
                    engine.requestQuit();
                    return;
                }
            }
        }, "agent-stdin");
        console.setDaemon(true);
        console.start();
        return engine.runAgent();
    }

    private int runInspector() {
        if (!engine.connect()) {
            return 1;
        }
        System.out.println("Perintah: LIST | SYSINFO <agentId> | PING <host> | RESOLVE <domain> | BROADCAST <teks> | MSG <username> <teks> | EXEC <agentId> <namaPerintah> | SUBSCRIBE <udpPort> | UNSUBSCRIBE | WATCH | QUIT");
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
            ClientEngine.Result r = engine.request(cmd);
            switch (r.status) {
                case OK:
                    System.out.println(r.line);
                    printHint(r.line);
                    // Buka socket UDP lokal setelah SUBSCRIBE berhasil (logika soket di ClientEngine, bukan UI).
                    String first = cmd.trim().split("\\s+")[0].toUpperCase();
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

    @Override
    public void onInfo(String message) {
        System.out.println(message);
    }

    @Override
    public void onError(String message) {
        System.err.println(message);
    }

    @Override
    public void onConnected(String loginResponse, boolean reconnected) {
        System.out.println("Server: " + loginResponse);
        if (reconnected) {
            System.out.println("Tersambung kembali ke server.");
        }
        if (agent) {
            System.out.println("Agent aktif. Ketik 'quit' atau 'exit' lalu Enter untuk keluar.");
        }
    }

    @Override
    public void onServerMessage(String line) {
        System.out.println("[Server] " + line);
    }

    @Override
    public void onChatMessage(String line) {
        // Pesan chat masuk (BCAST / MSG): tampilkan di bawah prompt tanpa menghapus input pengguna.
        System.out.println("[Chat] " + line);
    }

    @Override
    public void onTelemetry(String agentId, double cpuLoad, double memPercent, int seqNum) {
        // Telemetri UDP dari server: ringkasan singkat di konsol Inspector.
        System.out.println(String.format(java.util.Locale.US,
                "[Telemetry] %s: CPU %.2f%% | Mem %.2f%% (seq %d)",
                agentId, cpuLoad * 100.0, memPercent, seqNum));
    }

    @Override
    public void onConnectionLost(String reason, boolean willReconnect, boolean byServer) {
        if (willReconnect) {
            System.out.println("[KONEKSI TERPUTUS] Agent kehilangan koneksi ke server: " + reason + ".");
            return;
        }
        // Inspector: tidak menyambung ulang; tampilkan penyebab lalu hentikan program.
        System.out.println();
        System.out.println("[KONEKSI TERPUTUS] Koneksi ke server terputus: " + reason + ".");
        System.out.println(byServer
                ? "Jalankan ulang Inspector untuk login kembali."
                : "Jalankan ulang Inspector setelah server dan jaringan pulih.");
        System.exit(2);
    }

    @Override
    public void onReconnecting(int attempt, int maxAttempts, long delayMs) {
        System.out.println("Mencoba menyambung ulang dalam " + (delayMs / 1000.0) + " detik (percobaan "
                + attempt + "/" + maxAttempts + "). Ketik 'quit' atau 'exit' untuk berhenti.");
    }

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
        System.out.println("  SUBSCRIBE <udpPort>   (mulai terima telemetri UDP)");
        System.out.println("  UNSUBSCRIBE           (berhenti terima telemetri UDP)");
        System.out.println("  WATCH                 (metrik terakhir semua Agent)");
        System.out.println("  QUIT / EXIT");
    }
}
