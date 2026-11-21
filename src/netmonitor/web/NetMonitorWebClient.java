package netmonitor.web;

/**
 * Titik masuk Web GUI NetMonitor. Menjalankan {@link WebBridgeServer} pada port lokal (bawaan 8080,
 * auto-increment bila terpakai) dan mencetak URL yang dapat dibuka di browser.
 */
public final class NetMonitorWebClient {

    private static final int DEFAULT_PORT = 8080;

    private NetMonitorWebClient() {
        // kelas utilitas titik masuk
    }

    /**
     * Meluncurkan Web GUI.
     *
     * @param args argumen baris perintah; args[0] (opsional) port awal
     */
    public static void main(String[] args) {
        int preferred = DEFAULT_PORT;
        if (args.length >= 1) {
            try {
                preferred = Integer.parseInt(args[0].trim());
            } catch (NumberFormatException ignored) {
                // pakai default
            }
        }
        try {
            WebBridgeServer bridge = new WebBridgeServer(preferred);
            System.out.println("NetMonitor Web GUI berjalan di http://localhost:" + bridge.getPort());
            System.out.println("Tekan Ctrl+C untuk menghentikan.");
            Runtime.getRuntime().addShutdownHook(new Thread(bridge::stop, "web-shutdown"));
            // jaga proses tetap hidup
            while (true) {
                Thread.sleep(60_000L);
            }
        } catch (Exception e) {
            System.err.println("Gagal menjalankan Web GUI: " + e.getMessage());
            System.exit(1);
        }
    }
}