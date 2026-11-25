package tech.powerjob.server.auth.service.login.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.utils.DigestUtils;
import tech.powerjob.server.auth.common.AuthConstants;
import tech.powerjob.server.auth.common.PowerJobAuthException;
import tech.powerjob.server.auth.jwt.JwtService;
import tech.powerjob.server.auth.login.LoginTypeInfo;
import tech.powerjob.server.auth.login.ThirdPartyLoginRequest;
import tech.powerjob.server.auth.login.ThirdPartyLoginService;
import tech.powerjob.server.auth.login.ThirdPartyUser;
import tech.powerjob.server.auth.login.impl.PwjbAccountLoginService;
import tech.powerjob.server.auth.service.login.LoginRequest;
import tech.powerjob.server.persistence.remote.model.PwjbUserInfoDO;
import tech.powerjob.server.persistence.remote.repository.PwjbUserInfoRepository;
import tech.powerjob.server.persistence.remote.repository.UserInfoRepository;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PowerJobLoginServiceImplTest {

    private final JwtService jwtService = mock(JwtService.class);
    private final UserInfoRepository userInfoRepository = mock(UserInfoRepository.class);
    private final PwjbUserInfoRepository pwjbUserInfoRepository = mock(PwjbUserInfoRepository.class);
    private final PwjbAccountLoginService pwjbAccountLoginService = new PwjbAccountLoginService();
    private final ThirdPartyLoginService extraProvider = new ExtraProvider();
    private PowerJobLoginServiceImpl loginService;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        Field repositoryField = PwjbAccountLoginService.class.getDeclaredField("pwjbUserInfoRepository");
        repositoryField.setAccessible(true);
        repositoryField.set(pwjbAccountLoginService, pwjbUserInfoRepository);
        loginService = new PowerJobLoginServiceImpl(jwtService, userInfoRepository,
                List.of(pwjbAccountLoginService, extraProvider));
    }

    @Test
    void supportedLoginTypesContainsOnlyPowerJobAccount() {
        assertEquals(List.of(AuthConstants.ACCOUNT_TYPE_POWER_JOB),
                loginService.fetchSupportLoginTypes().stream().map(LoginTypeInfo::getType).toList());
    }

    @Test
    void unsupportedProviderCannotStartLogin() {
        assertThrows(PowerJobAuthException.class,
                () -> loginService.doLogin(new LoginRequest().setLoginType("DING")));
    }

    @Test
    void unsupportedProviderCannotGenerateLoginRedirect() {
        assertThrows(PowerJobAuthException.class, () -> loginService.fetchThirdPartyLoginUrl("DING", null));
    }

    @Test
    void powerJobLoginRedirectsToLocalUsernamePasswordForm() {
        assertEquals(AuthConstants.FE_REDIRECT_KEY + "powerjobLogin",
                loginService.fetchThirdPartyLoginUrl(AuthConstants.ACCOUNT_TYPE_POWER_JOB, null));
    }

    @Test
    void validPowerJobUsernameAndPasswordStillAuthenticates() {
        PwjbUserInfoDO account = new PwjbUserInfoDO();
        account.setUsername("alice");
        account.setPassword(DigestUtils.rePassword("secret", "alice"));
        when(pwjbUserInfoRepository.findByUsername("alice")).thenReturn(Optional.of(account));

        ThirdPartyUser result = pwjbAccountLoginService.login(new ThirdPartyLoginRequest()
                .setOriginParams("{\"username\":\"alice\",\"password\":\"secret\"}"));

        assertEquals("alice", result.getUsername());
    }

    @Test
    void incorrectPowerJobPasswordIsRejected() {
        PwjbUserInfoDO account = new PwjbUserInfoDO();
        account.setUsername("alice");
        account.setPassword(DigestUtils.rePassword("secret", "alice"));
        when(pwjbUserInfoRepository.findByUsername("alice")).thenReturn(Optional.of(account));

        assertThrows(PowerJobException.class, () -> pwjbAccountLoginService.login(new ThirdPartyLoginRequest()
                .setOriginParams("{\"username\":\"alice\",\"password\":\"wrong\"}")));
    }

    private static class ExtraProvider implements ThirdPartyLoginService {

        @Override
        public LoginTypeInfo loginType() {
            return new LoginTypeInfo().setType("DING").setName("DingTalk");
        }

        @Override
        public String generateLoginUrl(jakarta.servlet.http.HttpServletRequest httpServletRequest) {
            return "https://provider.example/login";
        }

        @Override
        public ThirdPartyUser login(ThirdPartyLoginRequest loginRequest) {
            return new ThirdPartyUser().setUsername("external-user");
        }
    }
}
