package tech.powerjob.server.web;

import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.RequestMapping;
import tech.powerjob.server.web.controller.AuthController;

import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertFalse;

class AuthControllerLoginTest {

    @Test
    void thirdPartyCallbackIsNotAnAuthenticationRoute() {
        boolean callbackMapped = Arrays.stream(AuthController.class.getDeclaredMethods())
                .map(method -> method.getAnnotation(RequestMapping.class))
                .filter(Objects::nonNull)
                .flatMap(mapping -> Arrays.stream(mapping.value()))
                .anyMatch("/thirdPartyLoginCallback"::equals);

        assertFalse(callbackMapped);
    }
}
