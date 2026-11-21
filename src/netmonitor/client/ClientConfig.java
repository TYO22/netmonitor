package netmonitor.client;

import netmonitor.common.Protocol;

/**
 * Pengaturan koneksi klien (tanpa logika). Dipakai bersama oleh mesin jaringan {@link ClientEngine}
 * dan lapisan tampilan (konsol sekarang, GUI web nanti), sehingga keduanya tidak saling bergantung.
 */
/** Pengaturan koneksi klien (tanpa logika). Dipakai bersama oleh mesin jaringan {@link ClientEngine} dan lapisan tampilan (konsol sekarang, GUI web nanti), sehingga keduanya tidak saling bergantung. */
public final class ClientConfig {

    private final String host;
    private final int port;
    private final String role;
    private final String username;
    private final String password;

    /**
     * Membuat konfigurasi koneksi klien.
     *
     * @param host     host server
     * @param port     port server
     * @param role     peran klien (AGENT/INSPECTOR)
     * @param username username login
     * @param password password login
     */
    public ClientConfig(String host, int port, String role, String username, String password) {
        this.host = host;
        this.port = port;
        this.role = role;
        this.username = username;
        this.password = password;
    }

    /** @return host server. */
    public String getHost() {
        return host;
    }

    /** @return port server. */
    public int getPort() {
        return port;
    }

    /** @return peran klien (AGENT atau INSPECTOR). */
    public String getRole() {
        return role;
    }

    /** @return username login. */
    public String getUsername() {
        return username;
    }

    /** @return password login. */
    public String getPassword() {
        return password;
    }

    /** @return true bila klien berperan AGENT. */
    public boolean isAgent() {
        return Protocol.ROLE_AGENT.equals(role);
    }
}
