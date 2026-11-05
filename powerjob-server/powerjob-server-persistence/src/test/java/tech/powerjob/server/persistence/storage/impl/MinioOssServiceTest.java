package tech.powerjob.server.persistence.storage.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.powerjob.server.extension.dfs.DownloadRequest;
import tech.powerjob.server.extension.dfs.FileLocation;
import tech.powerjob.server.extension.dfs.StoreRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Runs real signed SDK requests against a disposable path-style S3 HTTP fixture. */
class MinioOssServiceTest {
    @TempDir
    Path directory;

    @Test
    void preservesBucketPolicyFilesAndMetadataWithoutSwallowingDeniedAccess() throws Exception {
        AtomicBoolean exists = new AtomicBoolean();
        AtomicReference<byte[]> object = new AtomicReference<>();
        AtomicReference<String> policy = new AtomicReference<>();
        AtomicReference<Throwable> fixtureFailure = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                assertTrue(exchange.getRequestHeaders().getFirst("Authorization").startsWith("AWS4-HMAC-SHA256"));
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if (path.endsWith("/denied")) {
                    respond(exchange, 403, "<Error><Code>AccessDenied</Code><Message>Denied</Message></Error>");
                } else if (path.endsWith("/missing")) {
                    respond(exchange, 404, "<Error><Code>NoSuchKey</Code><Message>Missing</Message></Error>");
                } else if (exchange.getRequestURI().getQuery() != null
                        && exchange.getRequestURI().getQuery().startsWith("policy")) {
                    policy.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                    respond(exchange, 200, "");
                } else if (path.equals("/fixture-bucket") || path.equals("/fixture-bucket/")) {
                    if (method.equals("PUT")) {
                        exists.set(true);
                        exchange.getResponseHeaders().set("Location", "/fixture-bucket");
                        respond(exchange, 200, "");
                    } else {
                        respond(exchange, exists.get() ? 200 : 404, "");
                    }
                } else if (path.equals("/fixture-bucket/logical/file.txt")) {
                    if (method.equals("PUT")) {
                        byte[] bytes = exchange.getRequestBody().readAllBytes();
                        if ("aws-chunked".equals(exchange.getRequestHeaders().getFirst("Content-Encoding"))
                                || "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".equals(
                                exchange.getRequestHeaders().getFirst("x-amz-content-sha256"))) {
                            bytes = decodeChunks(bytes);
                        }
                        object.set(bytes);
                        exchange.getResponseHeaders().set("ETag", "\"fixture-etag\"");
                        respond(exchange, 200, "");
                    } else {
                        exchange.getResponseHeaders().set("Content-Length", Integer.toString(object.get().length));
                        exchange.getResponseHeaders().set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT");
                        exchange.getResponseHeaders().set("ETag", "\"fixture-etag\"");
                        exchange.getResponseHeaders().set("Cache-Control", "max-age=3600");
                        exchange.getResponseHeaders().set("x-amz-version-id", "fixture-version");
                        if (method.equals("HEAD")) {
                            respond(exchange, 200, "");
                        } else {
                            exchange.sendResponseHeaders(200, object.get().length);
                            exchange.getResponseBody().write(object.get());
                            exchange.close();
                        }
                    }
                } else {
                    throw new AssertionError("Unexpected S3 request: " + method + " " + exchange.getRequestURI());
                }
            } catch (Throwable failure) {
                fixtureFailure.set(failure);
                respond(exchange, 500, "");
            }
        });
        server.start();
        MinioOssService service = new MinioOssService();
        try {
            service.initOssClient("http://127.0.0.1:" + server.getAddress().getPort(), "fixture-bucket",
                    "synthetic-access", "synthetic-secret");
            assertTrue(exists.get());
            assertTrue(policy.get().contains("s3:GetObject"));
            assertTrue(policy.get().contains("arn:aws:s3:::fixture-bucket/*"));
            Path source = directory.resolve("source");
            Files.writeString(source, "processor-fixture");
            FileLocation location = new FileLocation().setBucket("logical").setName("file.txt");
            service.store(new StoreRequest().setFileLocation(location).setLocalFile(source.toFile()));
            Path target = directory.resolve("download/target");
            service.download(new DownloadRequest().setFileLocation(location).setTarget(target.toFile()));
            assertEquals("processor-fixture", Files.readString(target));
            var metadata = service.fetchFileMeta(location).orElseThrow();
            assertEquals(17L, metadata.getLength());
            assertEquals(1704067200000L, metadata.getLastModifiedTime().getTime());
            assertEquals("max-age=3600", metadata.getMetaInfo().get("Cache-Control"));
            assertEquals("fixture-version", metadata.getMetaInfo().get("x-amz-version-id"));
            assertTrue(service.fetchFileMeta(new FileLocation().setBucket("logical").setName("missing")).isEmpty());
            assertThrows(RuntimeException.class,
                    () -> service.fetchFileMeta(new FileLocation().setBucket("logical").setName("denied")));
            assertNull(fixtureFailure.get());
        } finally {
            service.destroy();
            server.stop(0);
        }
    }

    private static byte[] decodeChunks(byte[] bytes) {
        java.io.ByteArrayOutputStream decoded = new java.io.ByteArrayOutputStream();
        int offset = 0;
        while (offset < bytes.length) {
            int lineEnd = offset;
            while (bytes[lineEnd] != '\r' || bytes[lineEnd + 1] != '\n') {
                lineEnd++;
            }
            String header = new String(bytes, offset, lineEnd - offset, StandardCharsets.US_ASCII);
            int length = Integer.parseInt(header.split(";")[0], 16);
            if (length == 0) {
                break;
            }
            offset = lineEnd + 2;
            decoded.write(bytes, offset, length);
            offset += length + 2;
        }
        return decoded.toByteArray();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, exchange.getRequestMethod().equals("HEAD") || bytes.length == 0 ? -1 : bytes.length);
        if (!exchange.getRequestMethod().equals("HEAD")) {
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }
}