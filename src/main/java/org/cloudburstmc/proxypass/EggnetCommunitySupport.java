package org.cloudburstmc.proxypass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class EggnetCommunitySupport {
    public static final String LIST3_URL_DEFAULT = "https://eggnet.space/api/servers/list3";
    public static final String COMMUNITY_FOLLOWERS_URL_DEFAULT = "https://eggnet.space/api/community/followers";
    public static final String COMMUNITY_FOLLOWING_URL_DEFAULT = "https://eggnet.space/api/community/following";
    public static final String SELF_MINECRAFT_SESSION_URL_DEFAULT = "https://eggnet.space/api/xbl/self_minecraft_session";
    public static final String RANDOM_ACTOR_URL_DEFAULT = "https://eggnet.space/api/xbl/random_actor";
    public static final String DESKTOP_XUID_ENV = "EGGNET_DESKTOP_XUID";
    public static final String DESKTOP_XUID_FILE_ENV = "EGGNET_DESKTOP_XUID_FILE";
    public static final String DESKTOP_XUID_SYS_PROP = "eggnet.desktop.xuid";
    public static final String DESKTOP_XUID_FILE_SYS_PROP = "eggnet.desktop.xuidFile";
    public static final String[] DESKTOP_XUID_FILE_CANDIDATES = new String[]{
            "desktop_xuid.txt",
            "desktop_xuid.json",
            "xuid.txt",
            "xuid.json"
    };
    public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(8);
    public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration SELF_MINECRAFT_SESSION_REQUEST_TIMEOUT = Duration.ofSeconds(75);
    public static final int COMMUNITY_LIST_LIMIT = 200;
    public static final int COMMUNITY_MAX_PAGES = 10;
    public static final int ADVERTISEMENT_TRANSPORT_LAYER = 2;
    public static final int ADVERTISEMENT_CONNECTION_TYPE = 4;

    private EggnetCommunitySupport() {
    }

    public record SessionAuthorization(String authorization, long expiresAtMs) {
    }

    public record ActorSessionAuthorization(String xuid, String authorization, long expiresAtMs) {
    }

    public static String resolveDesktopXuid(ObjectMapper mapper) {
        String overrideXuid = normalizeXuid(firstNonBlank(
                System.getProperty(DESKTOP_XUID_SYS_PROP, ""),
                System.getenv(DESKTOP_XUID_ENV)
        ));
        if (hasText(overrideXuid)) {
            return overrideXuid;
        }

        Path overridePath = resolveDesktopXuidPathOverride();
        if (overridePath != null) {
            String xuid = readDesktopXuidFromFile(mapper, overridePath);
            if (hasText(xuid)) {
                return xuid;
            }
        }

        Path roamingDir = resolveDesktopRoamingDir();
        if (roamingDir != null) {
            for (String fileName : DESKTOP_XUID_FILE_CANDIDATES) {
                String xuid = readDesktopXuidFromFile(mapper, roamingDir.resolve(fileName));
                if (hasText(xuid)) {
                    return xuid;
                }
            }
        }

        Path runDir = Paths.get(".").toAbsolutePath().normalize();
        for (String fileName : DESKTOP_XUID_FILE_CANDIDATES) {
            String xuid = readDesktopXuidFromFile(mapper, runDir.resolve(fileName));
            if (hasText(xuid)) {
                return xuid;
            }
        }

        return "";
    }

    public static Set<String> fetchCommunityXuids(
            ObjectMapper mapper,
            HttpClient httpClient,
            String endpoint,
            String selfXuid
    ) throws IOException, InterruptedException {
        Set<String> out = new LinkedHashSet<>();
        long beforeMs = 0L;
        String refreshToken = Long.toUnsignedString(System.currentTimeMillis());
        for (int page = 0; page < COMMUNITY_MAX_PAGES; page++) {
            StringBuilder uriBuilder = new StringBuilder(endpoint)
                    .append("?xuid=").append(selfXuid)
                    .append("&limit=").append(COMMUNITY_LIST_LIMIT)
                    .append("&_rt=").append(refreshToken);
            if (beforeMs > 0L) {
                uriBuilder.append("&beforeMs=").append(beforeMs);
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(uriBuilder.toString()))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Accept", "application/json")
                    .header("X-XUID", selfXuid)
                    .header("Cache-Control", "no-cache, no-store")
                    .header("Pragma", "no-cache")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                throw new IOException("community_status_" + response.statusCode());
            }

            JsonNode root = mapper.readTree(response.body());
            JsonNode items = root.path("items");
            if (!items.isArray() || items.size() == 0) {
                break;
            }

            long minFollowedAtMs = Long.MAX_VALUE;
            int addedInPage = 0;
            for (JsonNode item : items) {
                String xuid = item.path("xuid").asText("").trim();
                if (hasText(xuid) && out.add(xuid)) {
                    addedInPage++;
                }
                long followedAtMs = item.path("followedAtMs").asLong(0L);
                if (followedAtMs > 0L && followedAtMs < minFollowedAtMs) {
                    minFollowedAtMs = followedAtMs;
                }
            }

            if (items.size() < COMMUNITY_LIST_LIMIT) {
                break;
            }
            if (minFollowedAtMs == Long.MAX_VALUE || minFollowedAtMs <= 0L || addedInPage == 0) {
                break;
            }
            beforeMs = minFollowedAtMs;
        }
        return out;
    }

    public static Map<String, LiveServerInfo> fetchLiveServersByNetherId(
            ObjectMapper mapper,
            HttpClient httpClient,
            String liveList3Url
    ) throws IOException, InterruptedException {
        Map<String, LiveServerInfo> byNether = new HashMap<>();
        String requestUrl = liveList3Url + (liveList3Url.contains("?") ? "&" : "?") + "_rt=" + Long.toUnsignedString(System.currentTimeMillis());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(requestUrl))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("Cache-Control", "no-cache, no-store")
                .header("Pragma", "no-cache")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IOException("list3_status_" + response.statusCode());
        }

        JsonNode root = mapper.readTree(response.body());
        JsonNode servers = root.path("servers");
        if (!servers.isArray()) {
            return byNether;
        }

        for (JsonNode serverNode : servers) {
            String netherId = serverNode.path("nethernetId").asText("").trim();
            if (!hasText(netherId)) {
                continue;
            }

            JsonNode noteNode = serverNode.get("note");
            if (noteNode == null || noteNode.isNull()) {
                continue;
            }

            JsonNode noteObj = noteNode;
            if (noteNode.isTextual()) {
                try {
                    noteObj = mapper.readTree(noteNode.asText());
                } catch (Exception ignored) {
                    continue;
                }
            }

            JsonNode handle = noteObj.path("world").path("handle");
            if (handle.isMissingNode() || handle.path("closed").asBoolean(false)) {
                continue;
            }

            String pmsgId = handle.path("pmsgId").asText("").trim();
            if (!hasText(pmsgId)) {
                continue;
            }
            String sessionScid = handle.path("sessionScid").asText(handle.path("scid").asText("")).trim();
            String sessionTemplateName = handle.path("sessionTemplateName").asText("").trim();
            String sessionName = handle.path("sessionName").asText("").trim();
            List2ServerMeta meta = parseList2ServerMeta(serverNode, handle);

            byNether.put(
                    netherId,
                    new LiveServerInfo(
                            netherId,
                            pmsgId,
                            sessionScid,
                            sessionTemplateName,
                            sessionName,
                            meta.ownerXuid,
                            meta.ownerGamertag,
                            meta.worldName,
                            meta.hostName,
                            meta.displayTitle,
                            meta.gameType,
                            meta.primaryLanguage,
                            meta.memberCount,
                            meta.maxMemberCount,
                            meta.connectionType,
                            meta.transportLayer
                    )
            );
        }

        return byNether;
    }

    public static SessionAuthorization fetchSelfMinecraftSessionAuthorization(
            ObjectMapper mapper,
            HttpClient httpClient,
            String xuid
    ) throws IOException, InterruptedException {
        String normalizedXuid = normalizeXuid(xuid);
        if (!hasText(normalizedXuid)) {
            throw new IOException("missing_xuid");
        }

        String payload = "{\"xuid\":\"" + normalizedXuid + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(SELF_MINECRAFT_SESSION_URL_DEFAULT))
                .timeout(SELF_MINECRAFT_SESSION_REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("X-XUID", normalizedXuid)
                .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IOException("self_minecraft_session_status_" + response.statusCode());
        }

        JsonNode root = mapper.readTree(response.body());
        if (root == null || !root.path("ok").asBoolean(false)) {
            throw new IOException("self_minecraft_session_rejected");
        }

        String authorization = root.path("authorization").asText("").trim();
        if (!authorization.startsWith("MCToken ")) {
            throw new IOException("self_minecraft_session_invalid_auth");
        }

        long expiresAtMs = Math.max(0L, root.path("expiresAtMs").asLong(0L));
        return new SessionAuthorization(authorization, expiresAtMs);
    }

    public static ActorSessionAuthorization fetchRandomActorAuthorization(
            ObjectMapper mapper,
            HttpClient httpClient,
            long minTtlMs
    ) throws IOException, InterruptedException {
        long safeMinTtlMs = Math.max(0L, minTtlMs);
        String requestUrl = RANDOM_ACTOR_URL_DEFAULT + "?minTtlMs=" + safeMinTtlMs;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(requestUrl))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .header("Cache-Control", "no-cache, no-store")
                .header("Pragma", "no-cache")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IOException("random_actor_status_" + response.statusCode());
        }

        JsonNode root = mapper.readTree(response.body());
        if (root == null || !root.path("ok").asBoolean(false)) {
            throw new IOException(firstNonBlank(
                    root == null ? "" : root.path("error").asText(""),
                    "random_actor_rejected"
            ));
        }

        String xuid = normalizeXuid(root.path("xuid").asText(""));
        String authorization = firstNonBlank(
                root.path("authorization").asText(""),
                root.path("authHeader").asText("")
        );
        if (!authorization.startsWith("MCToken ")) {
            throw new IOException("random_actor_invalid_auth");
        }

        long expiresAtMs = Math.max(0L, root.path("expiresAtMs").asLong(0L));
        return new ActorSessionAuthorization(xuid, authorization, expiresAtMs);
    }

    private static List2ServerMeta parseList2ServerMeta(JsonNode serverNode, JsonNode handle) {
        String ownerXuid = firstNonBlank(
                serverNode.path("ownerXuid").asText(""),
                handle.path("ownerId").asText(""),
                handle.path("ownerXuid").asText("")
        );
        String ownerGamertag = firstNonBlank(
                serverNode.path("ownerGamertag").asText(""),
                serverNode.path("hostName").asText(""),
                handle.path("hostName").asText("")
        );
        String worldName = firstNonBlank(
                handle.path("worldName").asText(""),
                serverNode.path("title").asText(""),
                handle.path("worldType").asText("")
        );
        String hostName = firstNonBlank(
                handle.path("hostName").asText(""),
                serverNode.path("hostName").asText(""),
                ownerGamertag
        );
        String displayTitle = firstNonBlank(worldName, hostName, ownerGamertag, "World");
        int gameType = parseGameType(firstNonBlank(
                handle.path("worldType").asText(""),
                serverNode.path("worldType").asText(""),
                serverNode.path("mode").asText(""),
                serverNode.path("gameType").asText("")
        ));
        String primaryLanguage = normalizeLanguageCode(firstNonBlank(
                serverNode.path("languageSchema").path("primary").asText(""),
                serverNode.path("lang").asText(""),
                serverNode.path("language").asText("")
        ));

        int memberCount = Math.max(
                readIntOrDefault(handle, "memberCount", readIntOrDefault(handle, "membersCount", 0)),
                0
        );
        int maxMemberCount = Math.max(
                readIntOrDefault(handle, "maxMemberCount", readIntOrDefault(handle, "maxMembersCount", 10)),
                1
        );
        int connectionType = readIntOrDefault(handle, "connectionType", 4);
        int transportLayer = readIntOrDefault(handle, "transportLayer", 2);

        return new List2ServerMeta(
                ownerXuid,
                ownerGamertag,
                worldName,
                hostName,
                displayTitle,
                gameType,
                primaryLanguage,
                memberCount,
                maxMemberCount,
                connectionType,
                transportLayer
        );
    }

    public static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    public static String firstNonBlank(String... candidates) {
        if (candidates == null) {
            return "";
        }
        for (String candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            String trimmed = candidate.trim();
            if (!trimmed.isEmpty()) {
                return trimmed;
            }
        }
        return "";
    }

    public static String normalizeXuid(String raw) {
        if (!hasText(raw)) {
            return "";
        }
        String trimmed = raw.trim();
        boolean allDigits = true;
        for (int i = 0; i < trimmed.length(); i++) {
            if (!Character.isDigit(trimmed.charAt(i))) {
                allDigits = false;
                break;
            }
        }
        if (allDigits && trimmed.length() >= 10) {
            return trimmed;
        }

        StringBuilder digits = new StringBuilder(trimmed.length());
        for (int i = 0; i < trimmed.length(); i++) {
            char ch = trimmed.charAt(i);
            if (Character.isDigit(ch)) {
                digits.append(ch);
            }
        }
        return digits.length() >= 10 ? digits.toString() : "";
    }

    public static String normalizeLanguageCode(String raw) {
        if (!hasText(raw)) {
            return "";
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT).replace('_', '-');
        int cut = normalized.indexOf('-');
        if (cut > 0) {
            normalized = normalized.substring(0, cut);
        }
        return normalized;
    }

    private static int readIntOrDefault(JsonNode node, String key, int fallback) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return fallback;
        }
        JsonNode child = node.path(key);
        if (child.isMissingNode() || child.isNull()) {
            return fallback;
        }
        return child.asInt(fallback);
    }

    private static int parseGameType(String raw) {
        String value = firstNonBlank(raw).toLowerCase(Locale.ROOT);
        if (value.isEmpty()) {
            return 0;
        }
        return switch (value) {
            case "creative" -> 1;
            case "adventure" -> 2;
            case "spectator" -> 3;
            default -> 0;
        };
    }

    private static Path resolveDesktopXuidPathOverride() {
        String overridePath = firstNonBlank(
                System.getProperty(DESKTOP_XUID_FILE_SYS_PROP, ""),
                System.getenv(DESKTOP_XUID_FILE_ENV)
        );
        if (!hasText(overridePath)) {
            return null;
        }
        try {
            return Paths.get(overridePath);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static Path resolveDesktopRoamingDir() {
        String appData = System.getenv("APPDATA");
        if (hasText(appData)) {
            return Paths.get(appData, "Eggnet Arcade");
        }

        String home = System.getenv("HOME");
        if (hasText(home)) {
            return Paths.get(home, ".config", "Eggnet Arcade");
        }
        return null;
    }

    private static String readDesktopXuidFromFile(ObjectMapper mapper, Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return "";
        }
        try {
            String raw = Files.readString(path, StandardCharsets.UTF_8).trim();
            if (!hasText(raw)) {
                return "";
            }
            if (raw.startsWith("{") && raw.endsWith("}")) {
                JsonNode node = mapper.readTree(raw);
                return normalizeXuid(firstNonBlank(
                        node.path("xuid").asText(""),
                        node.path("userXUID").asText(""),
                        node.path("selectedXuid").asText(""),
                        node.path("currentXuid").asText("")
                ));
            }
            return normalizeXuid(raw);
        } catch (Exception ignored) {
            return "";
        }
    }

    public record LiveServerInfo(
            String nethernetId,
            String pmsgId,
            String sessionScid,
            String sessionTemplateName,
            String sessionName,
            String ownerXuid,
            String ownerGamertag,
            String worldName,
            String hostName,
            String displayTitle,
            int gameType,
            String primaryLanguage,
            int memberCount,
            int maxMemberCount,
            int connectionType,
            int transportLayer
    ) {
    }

    private record List2ServerMeta(
            String ownerXuid,
            String ownerGamertag,
            String worldName,
            String hostName,
            String displayTitle,
            int gameType,
            String primaryLanguage,
            int memberCount,
            int maxMemberCount,
            int connectionType,
            int transportLayer
    ) {
    }
}
