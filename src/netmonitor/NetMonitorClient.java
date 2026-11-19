package netmonitor;

/**
 * Titik masuk klien NetMonitor (Pertemuan 5). Sengaja tipis: pengaturan dibaca oleh tampilan konsol,
 * lalu {@link ConsoleClientUI} menjalankan {@link ClientEngine} (mesin jaringan).
 *
 * Jalankan: java -cp "out;lib\gson-2.15.0.jar" netmonitor.NetMonitorClient [host port peran username password]
 */
public class NetMonitorClient {

    public static void main(String[] args) {
        ClientConfig cfg = ConsoleClientUI.promptConfig(args);
        int exitCode = ConsoleClientUI.run(cfg);
        System.exit(exitCode);
    }
}
