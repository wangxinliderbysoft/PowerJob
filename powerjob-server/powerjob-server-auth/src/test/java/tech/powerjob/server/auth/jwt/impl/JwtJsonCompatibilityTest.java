package tech.powerjob.server.auth.jwt.impl;

import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.security.SignatureException;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.serialize.JsonUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Independent RFC 7519 fixtures retain legacy claim types and HS256 key derivation. */
class JwtJsonCompatibilityTest {
    private static final String SECRET = "syntheticJWTkey01234567890123456789";
    private static final String LEGACY_BASE = "CengMengXiangZhangJianZouTianYaKanYiKanShiJieDeFanHua"
            + "NianShaoDeXinZongYouXieQingKuangRuJinWoSiHaiWeiJia";

    @Test
    void parsesLegacyClaimsAndRejectsExpiredOrTamperedTokens() throws Exception {
        Map<String, Object> parsed = JwtServiceImpl.innerParse(SECRET, fixture(4102444800L));
        assertEquals(9007199254740993L, parsed.get("uid"));
        assertEquals("PowerJob", parsed.get("sub"));
        assertEquals(List.of(1, "value", true), ((Map<?, ?>) parsed.get("nested")).get("list"));
        assertTrue(((Map<?, ?>) parsed.get("nested")).containsKey("optional"));
        assertNull(((Map<?, ?>) parsed.get("nested")).get("optional"));
        assertThrows(ExpiredJwtException.class, () -> JwtServiceImpl.innerParse(SECRET, fixture(1)));
        String token = fixture(4102444800L);
        int signatureStart = token.lastIndexOf('.') + 1;
        String tampered = token.substring(0, signatureStart) + (token.charAt(signatureStart) == 'A' ? "B" : "A")
                + token.substring(signatureStart + 1);
        assertThrows(SignatureException.class, () -> JwtServiceImpl.innerParse(SECRET, tampered));
    }

    @Test
    void writesCompatibleNestedNullsNumbersAndNumericDates() throws Exception {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("optional", null);
        nested.put("list", List.of(1, "value", true));
        String token = JwtServiceImpl.innerBuild(SECRET, 60, Map.of("uid", 9007199254740993L, "nested", nested));
        String[] parts = token.split("\\.");
        assertEquals(parts[2], sign(parts[0] + "." + parts[1]));
        Map<String, Object> body = JsonUtils.parseMap(new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
        assertEquals(9007199254740993L, body.get("uid"));
        assertTrue(body.get("exp") instanceof Number);
        assertEquals(nested, body.get("nested"));
        assertEquals("PowerJob", body.get("sub"));
    }

    private static String fixture(long expiration) throws Exception {
        String header = encode("{\"typ\":\"JWT\",\"alg\":\"HS256\"}");
        String body = encode("{\"uid\":9007199254740993,\"nested\":{\"optional\":null,\"list\":[1,\"value\",true]},"
                + "\"sub\":\"PowerJob\",\"exp\":" + expiration + ",\"jti\":\"legacy-fixture\"}");
        return header + "." + body + "." + sign(header + "." + body);
    }

    private static String sign(String unsigned) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(Base64.getDecoder().decode(LEGACY_BASE + SECRET), "HmacSHA256"));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(unsigned.getBytes(StandardCharsets.US_ASCII)));
    }

    private static String encode(String json) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }
}
