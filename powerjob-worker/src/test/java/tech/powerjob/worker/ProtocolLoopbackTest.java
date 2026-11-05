package tech.powerjob.worker;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.remote.framework.BenchmarkActor.BenchmarkRequest;
import tech.powerjob.remote.framework.BenchmarkActor.BenchmarkResponse;
import tech.powerjob.remote.framework.actor.Actor;
import tech.powerjob.remote.framework.actor.Handler;
import tech.powerjob.remote.framework.base.Address;
import tech.powerjob.remote.framework.base.HandlerLocation;
import tech.powerjob.remote.framework.base.ServerType;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.remote.framework.engine.EngineConfig;
import tech.powerjob.remote.framework.engine.impl.PowerJobRemoteEngine;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exchanges requests across two real loopback endpoints for every supported transport.
 */
class ProtocolLoopbackTest {

    @Test
    void muReleasesAllThreadsAfterAnExchange(@TempDir Path directory) throws Exception {
        Path log = directory.resolve("mu-process.log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-XX:ActiveProcessorCount=2", "-cp", System.getProperty("java.class.path"),
                ProtocolLoopbackTest.class.getName(), Protocol.MU.name())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), () -> {
                try {
                    return "MU did not release its threads:\n" + Files.readString(log);
                } catch (java.io.IOException exception) {
                    throw new java.io.UncheckedIOException(exception);
                }
            });
            assertEquals(0, process.exitValue(), Files.readString(log));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new ProtocolLoopbackTest().exchange(Protocol.valueOf(args[0]));
    }

    @TestFactory
    Stream<DynamicTest> allProtocols() {
        return Arrays.stream(Protocol.values()).map(protocol ->
                DynamicTest.dynamicTest(protocol.name(), () -> exchange(protocol)));
    }

    private void exchange(Protocol protocol) throws Exception {
        Address serverAddress = address();
        PowerJobRemoteEngine server = new PowerJobRemoteEngine();
        PowerJobRemoteEngine worker = new PowerJobRemoteEngine();
        try {
            Transporter serverTransport = server.start(config(protocol, serverAddress, ServerType.SERVER)).getTransporter();
            Address workerAddress = address();
            Transporter workerTransport = worker.start(config(protocol, workerAddress, ServerType.WORKER)).getTransporter();
            assertResponse(workerTransport, serverAddress, ServerType.SERVER);
            assertResponse(serverTransport, workerAddress, ServerType.WORKER);
        } finally {
            try {
                worker.close();
            } finally {
                server.close();
            }
        }
    }

    private void assertResponse(Transporter transporter, Address address, ServerType serverType) throws Exception {
        URL url = new URL().setAddress(address).setServerType(serverType)
                .setLocation(new HandlerLocation().setRootPath("platformContract").setMethodPath("echo"));
        BenchmarkResponse response = transporter.ask(url, new BenchmarkRequest().setContent("java21-9007199254740993"),
                BenchmarkResponse.class).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertTrue(response.isSuccess());
        assertEquals("java21-9007199254740993", response.getContent());
    }

    private EngineConfig config(Protocol protocol, Address address, ServerType serverType) {
        return new EngineConfig().setType(protocol.name()).setBindAddress(address).setServerType(serverType)
                .setActorList(new ArrayList<>(List.of(new EchoActor())));
    }

    private Address address() throws java.io.IOException {
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"))) {
            return new Address().setHost("127.0.0.1").setPort(socket.getLocalPort());
        }
    }

    @Actor(path = "platformContract")
    public static class EchoActor {
        @Handler(path = "echo")
        public BenchmarkResponse echo(BenchmarkRequest request) {
            return new BenchmarkResponse().setSuccess(true).setContent(request.getContent());
        }
    }
}
