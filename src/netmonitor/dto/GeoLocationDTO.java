package netmonitor.dto;

import com.google.gson.annotations.SerializedName;

/**
 * DTO (Data Transfer Object / POJO) yang memetakan JSON balasan REST API GeoIP (ip-api.com).
 *
 * Pilar 4 (Final): JSON dideserialisasi otomatis oleh Gson lewat {@code gson.fromJson(json, GeoLocationDTO.class)}.
 * Anotasi {@link SerializedName} menghubungkan KUNCI JSON dengan field Java. Anotasi ini wajib bila nama
 * field Java berbeda dari kunci JSON (contoh: kunci "regionName" dipetakan ke field {@code region}), dan
 * dipasang pada semua field agar pemetaannya eksplisit. Kunci JSON yang tidak ada di kelas ini diabaikan.
 */
public class GeoLocationDTO {

    @SerializedName("status")
    private String status;        // "success" atau "fail"

    @SerializedName("message")
    private String message;       // alasan bila status = "fail"

    @SerializedName("country")
    private String country;

    @SerializedName("countryCode")
    private String countryCode;

    @SerializedName("regionName")
    private String region;        // nama field Java berbeda dari kunci JSON

    @SerializedName("city")
    private String city;

    @SerializedName("isp")
    private String isp;           // penyedia internet (membantu menjelaskan mengapa lokasi IP bisa meleset)

    public GeoLocationDTO() {
        // konstruktor kosong dibutuhkan Gson
    }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public String getCountryCode() { return countryCode; }
    public void setCountryCode(String countryCode) { this.countryCode = countryCode; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getIsp() { return isp; }
    public void setIsp(String isp) { this.isp = isp; }

    /** True bila API menjawab status "success". */
    public boolean isSuccess() {
        return "success".equals(status);
    }

    @Override
    public String toString() {
        return "GeoLocationDTO{status=" + status + ", city=" + city + ", region=" + region
                + ", countryCode=" + countryCode + ", isp=" + isp + "}";
    }
}
