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

    private static final class Account {
        final String password;
        final String role;

        Account(String password, String role) {
            this.password = password;
            this.role = role;
        }
    }

    private final Map<String, Account> accounts = new HashMap<>();

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

    /** True bila username ada, password cocok, dan peran sesuai dengan akun. */
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
