package netmonitor.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import netmonitor.common.Protocol;
import netmonitor.util.FileLogger;

/**
 * Otentikasi sederhana. Format berkas users.properties:
 *     username=password,ROLE        (ROLE = AGENT atau INSPECTOR)
 * Bila berkas tidak ada, dipakai akun bawaan untuk demo.
 */
public final class AuthService {

    /** Data akun tunggal: password dan peran. */
    private static final class Account {
        /** Password akun. */
        final String password;
        /** Peran akun (AGENT atau INSPECTOR). */
        final String role;

        /**
         * @param password password akun
         * @param role     peran akun
         */
        Account(String password, String role) {
            this.password = password;
            this.role = role;
        }
    }

    /** Peta username ke akun, diisi dari berkas atau akun bawaan. */
    private final Map<String, Account> accounts = new HashMap<>();

    /**
     * Membaca akun dari berkas; bila tidak ada atau kosong, memakai akun bawaan demo.
     *
     * @param file path berkas users.properties
     * @param log  pencatat log untuk pesan informasi/pemuatannya
     */
    public AuthService(String file, FileLogger log) {
        File f = new File(file);
        if (f.isFile()) {
            load(f, log);
        }
        if (accounts.isEmpty()) {
            log.warn("Berkas akun '" + file + "' tidak ada/kosong, memakai akun bawaan demo");
            accounts.put("admin", new Account("admin123", Protocol.ROLE_INSPECTOR));
            accounts.put("agent01", new Account("agent123", Protocol.ROLE_AGENT));
            accounts.put("agent02", new Account("agent123", Protocol.ROLE_AGENT));
            accounts.put("agent03", new Account("agent123", Protocol.ROLE_AGENT));
        } else {
            log.info("Memuat " + accounts.size() + " akun dari " + file);
        }
    }

    /**
     * Memuat akun dari berkas teks berformat {@code username=password,ROLE}.
     *
     * @param f   berkas akun
     * @param log pencatat log untuk peringatan baris tidak valid
     */
    private void load(File f, FileLogger log) {
        try (BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int eq = line.indexOf('=');
                int comma = line.lastIndexOf(',');
                if (eq < 1 || comma < eq + 2) {
                    log.warn("Baris akun diabaikan (format salah)");
                    continue;
                }
                String user = line.substring(0, eq).trim();
                String pass = line.substring(eq + 1, comma).trim();
                String role = line.substring(comma + 1).trim().toUpperCase();
                if (Protocol.isValidUsername(user) && Protocol.isValidRole(role) && !pass.isEmpty()) {
                    accounts.put(user, new Account(pass, role));
                } else {
                    log.warn("Akun '" + user + "' diabaikan (data tidak valid)");
                }
            }
        } catch (IOException e) {
            log.error("Gagal membaca berkas akun: " + e.getMessage());
        }
    }

    /**
     * Memeriksa apakah username ada, password cocok, dan peran sesuai dengan akun.
     *
     * @param username username yang diautentikasi
     * @param password password yang diautentikasi
     * @param role     peran yang diminta (AGENT atau INSPECTOR)
     * @return {@code true} bila semua cocok
     */
    public boolean authenticate(String username, String password, String role) {
        Account a = accounts.get(username);
        if (a == null) {
            return false;
        }
        boolean passOk = MessageDigest.isEqual(
                a.password.getBytes(StandardCharsets.UTF_8), password.getBytes(StandardCharsets.UTF_8));
        return passOk && a.role.equals(role);
    }
}
