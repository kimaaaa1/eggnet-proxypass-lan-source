package org.cloudburstmc.proxypass.tools;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.lenni0451.commons.httpclient.HttpClient;
import net.raphimc.minecraftauth.MinecraftAuth;
import net.raphimc.minecraftauth.bedrock.BedrockAuthManager;
import net.raphimc.minecraftauth.util.MinecraftAuth4To5Migrator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

public final class BuildMCTokenCli {
    private static final String TEMPLATE_ONLY_FLAG = "--template-only";
    private static final String DEFAULT_MSA_CLIENT_ID = "00000000441cc96b";
    private static final String DEFAULT_MSA_SCOPE = "service::user.auth.xboxlive.com::MBI_SSL";
    private static final String DEFAULT_GAME_VERSION = "1.26.0";
    private static final String DEFAULT_TEMPLATE_PATH = "proxypass-auth-template.json";

    private BuildMCTokenCli() {
    }

    public static void main(String[] args) {
        boolean templateOnly = args.length > 0 && TEMPLATE_ONLY_FLAG.equals(safe(args[0]));
        String refreshTokenB64 = templateOnly ? "" : (args.length > 0 ? safe(args[0]) : "");
        String templatePathRaw = templateOnly
                ? (args.length > 1 ? safe(args[1]) : "")
                : (args.length > 1 ? safe(args[1]) : "");
        String gameVersion = DEFAULT_GAME_VERSION;

        String refreshToken = "";
        if (!templateOnly) {
            if (refreshTokenB64.isEmpty()) {
                fail("missing_refresh_b64");
                return;
            }
            try {
                refreshToken = new String(Base64.getDecoder().decode(refreshTokenB64), StandardCharsets.UTF_8).trim();
            } catch (Exception e) {
                fail("refresh_b64_decode_failed:" + safe(e.getMessage()));
                return;
            }
            if (refreshToken.isEmpty()) {
                fail("empty_refresh_token");
                return;
            }
        }

        HttpClient client = null;
        try {
            client = MinecraftAuth.createHttpClient();
            Path templatePath = resolveTemplatePath(templatePathRaw);
            JsonObject templateJson = loadTemplateJson(templatePath);
            if (!templateOnly) {
                applyRefreshToken(templateJson, refreshToken);
            }
            BedrockAuthManager authManager = BedrockAuthManager.fromJson(client, gameVersion, templateJson);
            authManager.getMinecraftSession().refresh();
            authManager.getMinecraftCertificateChain().refresh();
            authManager.getMinecraftMultiplayerToken().refresh();
            authManager.getRealmsXstsToken().refresh();
            authManager.getXboxLiveXstsToken().refresh();

            String authorization = authManager.getMinecraftSession().getUpToDate().getAuthorizationHeader();
            if (authorization == null || !authorization.startsWith("MCToken ")) {
                fail("invalid_mctoken_prefix");
                return;
            }
            String xuid = safe(authManager.getMinecraftMultiplayerToken().getUpToDate().getXuid());
            if (xuid.isEmpty()) {
                fail("missing_xuid");
                return;
            }

            long expiresAtMs = 0L;
            String validUntil = "";
            try {
                JsonObject json = BedrockAuthManager.toJson(authManager);
                if (json.has("minecraftSession") && json.get("minecraftSession").isJsonObject()) {
                    JsonObject session = json.getAsJsonObject("minecraftSession");
                    if (session.has("expireTimeMs")) {
                        expiresAtMs = session.get("expireTimeMs").getAsLong();
                    }
                }
            } catch (Exception ignored) {
            }
            if (expiresAtMs > 0L) {
                validUntil = Long.toString(expiresAtMs);
            }

            out("ok=1");
            out("xuid=" + xuid);
            out("mctoken_b64=" + Base64.getEncoder().encodeToString(authorization.getBytes(StandardCharsets.UTF_8)));
            out("expires_at_ms=" + expiresAtMs);
            out("valid_until=" + validUntil);
        } catch (Exception e) {
            fail("mctoken_build_failed:" + safe(e.getMessage()));
        }
    }

    private static Path resolveTemplatePath(String raw) {
        String candidate = safe(raw);
        if (!candidate.isEmpty()) {
            return Paths.get(candidate).toAbsolutePath().normalize();
        }
        return Paths.get(DEFAULT_TEMPLATE_PATH).toAbsolutePath().normalize();
    }

    private static JsonObject loadTemplateJson(Path templatePath) throws Exception {
        if (!Files.exists(templatePath) || !Files.isRegularFile(templatePath)) {
            throw new IllegalStateException("auth_template_missing");
        }
        String accountString = Files.readString(templatePath, StandardCharsets.UTF_8);
        JsonObject accountJson = JsonParser.parseString(accountString).getAsJsonObject();
        if (accountJson.has("mcChain")) {
            accountJson = MinecraftAuth4To5Migrator.migrateBedrockSave(accountJson);
        }
        return accountJson;
    }

    private static void applyRefreshToken(JsonObject json, String refreshToken) {
        JsonObject msaConfig = ensureObject(json, "msaApplicationConfig");
        msaConfig.addProperty("_saveVersion", 1);
        msaConfig.addProperty("clientId", DEFAULT_MSA_CLIENT_ID);
        msaConfig.addProperty("scope", DEFAULT_MSA_SCOPE);

        JsonObject msaToken = ensureObject(json, "msaToken");
        msaToken.addProperty("_saveVersion", 1);
        msaToken.addProperty("refreshToken", refreshToken);
        if (msaToken.has("refresh_token")) {
            msaToken.addProperty("refresh_token", refreshToken);
        }
        if (msaToken.has("accessToken")) {
            msaToken.addProperty("accessToken", "");
        }
        if (msaToken.has("access_token")) {
            msaToken.addProperty("access_token", "");
        }
        msaToken.addProperty("expireTimeMs", 0);

        zeroExpire(json, "xblDeviceToken");
        zeroExpire(json, "xblUserToken");
        zeroExpire(json, "xblTitleToken");
        zeroExpire(json, "bedrockXstsToken");
        zeroExpire(json, "playFabXstsToken");
        zeroExpire(json, "realmsXstsToken");
        zeroExpire(json, "xboxLiveXstsToken");
        zeroExpire(json, "minecraftSession");
        zeroExpire(json, "minecraftMultiplayerToken");
        zeroExpire(json, "minecraftCertificateChain");

        if (json.has("playFabToken") && json.get("playFabToken").isJsonObject()) {
            JsonObject playFabToken = json.getAsJsonObject("playFabToken");
            if (playFabToken.has("expireTimeMs")) {
                playFabToken.addProperty("expireTimeMs", 0);
            }
            if (playFabToken.has("entityToken") && playFabToken.get("entityToken").isJsonObject()) {
                JsonObject entityToken = playFabToken.getAsJsonObject("entityToken");
                if (entityToken.has("expireTimeMs")) {
                    entityToken.addProperty("expireTimeMs", 0);
                }
            }
        }
    }

    private static JsonObject ensureObject(JsonObject parent, String name) {
        if (!parent.has(name) || !parent.get(name).isJsonObject()) {
            JsonObject value = new JsonObject();
            parent.add(name, value);
            return value;
        }
        return parent.getAsJsonObject(name);
    }

    private static void zeroExpire(JsonObject root, String name) {
        if (!root.has(name) || !root.get(name).isJsonObject()) {
            return;
        }
        JsonObject object = root.getAsJsonObject(name);
        if (object.has("expireTimeMs")) {
            object.addProperty("expireTimeMs", 0);
        }
    }

    private static String safe(String value) {
        return value == null ? "" : value.trim();
    }

    private static void out(String line) {
        System.out.println(line == null ? "" : line);
    }

    private static void fail(String message) {
        out("ok=0");
        out("error=" + safe(message));
    }
}
