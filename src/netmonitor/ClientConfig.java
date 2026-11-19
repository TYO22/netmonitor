package netmonitor;

/**
 * Pengaturan koneksi klien (tanpa logika). Dipakai bersama oleh mesin jaringan {@link ClientEngine}
 * dan lapisan tampilan (konsol sekarang, GUI web nanti), sehingga keduanya tidak saling bergantung.
 */
public final class ClientConfig {

    private final String host;
    private final int port;
    private final String role;
    private final String username;
    private final String password;

    public ClientConfig(String host, int port, String role, String username, String password) {
        this.host = host;
        this.port = port;
        this.role = role;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /** AGENT atau INSPECTOR. */
    public String getRole() {
        return role;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isAgent() {
        return Protocol.ROLE_AGENT.equals(role);
    }
}
