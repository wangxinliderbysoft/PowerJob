package tech.powerjob.server.persistence.storage.impl;

import com.google.inject.TypeLiteral;
import com.google.common.base.Function;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.jclouds.blobstore.BlobStoreFallbacks;
import org.jclouds.http.HttpRequest;
import org.jclouds.http.HttpResponse;
import org.jclouds.rest.ConfiguresHttpApi;
import org.jclouds.rest.InvocationContext;
import org.jclouds.rest.annotations.BinderParam;
import org.jclouds.rest.annotations.EndpointParam;
import org.jclouds.rest.annotations.Fallback;
import org.jclouds.rest.annotations.QueryParams;
import org.jclouds.rest.annotations.RequestFilters;
import org.jclouds.rest.annotations.ResponseParser;
import org.jclouds.rest.binders.BindToStringPayload;
import org.jclouds.s3.Bucket;
import org.jclouds.s3.S3ApiMetadata;
import org.jclouds.s3.S3Client;
import org.jclouds.s3.binders.BindAsHostPrefixIfConfigured;
import org.jclouds.s3.blobstore.config.S3BlobStoreContextModule;
import org.jclouds.s3.config.S3HttpApiModule;
import org.jclouds.s3.filters.RequestAuthorizeSignature;
import org.jclouds.s3.functions.AssignCorrectHostnameForBucket;
import org.jclouds.s3.functions.ParseObjectMetadataFromHeaders;
import tech.powerjob.server.extension.dfs.FileMeta;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Adds bucket policies while retaining jclouds' S3 binding, error handling and SigV4 signer. */
@RequestFilters(RequestAuthorizeSignature.class)
public interface MinioS3Client extends S3Client {
    @Named("GetObject")
    @HEAD
    @Path("/{key}")
    @Fallback(BlobStoreFallbacks.NullOnKeyNotFound.class)
    @ResponseParser(FileMetadataParser.class)
    FileMeta headFileMeta(@Bucket @EndpointParam(parser = AssignCorrectHostnameForBucket.class)
                          @BinderParam(BindAsHostPrefixIfConfigured.class) String bucket,
                          @PathParam("key") String key);

    @PUT
    @Path("/")
    @QueryParams(keys = "policy")
    void putBucketPolicy(@Bucket @EndpointParam(parser = AssignCorrectHostnameForBucket.class)
                         @BinderParam(BindAsHostPrefixIfConfigured.class) String bucket,
                         @BinderParam(BindToStringPayload.class) String policy);

    /** Retains raw response headers alongside the SDK's typed size and modification time. */
    final class FileMetadataParser implements Function<HttpResponse, FileMeta>, InvocationContext<FileMetadataParser> {
        private final ParseObjectMetadataFromHeaders delegate;

        @Inject
        public FileMetadataParser(ParseObjectMetadataFromHeaders delegate) {
            this.delegate = delegate;
        }

        @Override
        public FileMeta apply(HttpResponse response) {
            var metadata = delegate.apply(response);
            Map<String, Object> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            response.getHeaders().entries().forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
            headers.put("Content-Length", metadata.getContentMetadata().getContentLength());
            headers.put("Last-Modified", metadata.getLastModified());
            headers.put("ETag", metadata.getETag());
            headers.put("Content-Type", metadata.getContentMetadata().getContentType());
            return new FileMeta().setLastModifiedTime(metadata.getLastModified())
                    .setLength(metadata.getContentMetadata().getContentLength()).setMetaInfo(headers);
        }

        @Override
        public FileMetadataParser setContext(HttpRequest request) {
            delegate.setContext(request);
            return this;
        }
    }

    /** Registers the extended API instead of binding a second conflicting S3 module. */
    final class Metadata extends S3ApiMetadata {
        public Metadata() {
            super(new Builder());
        }

        private static final class Builder extends S3ApiMetadata.Builder<MinioS3Client, Builder> {
            private Builder() {
                super(MinioS3Client.class);
                defaultModules(Set.of(Module.class, S3BlobStoreContextModule.class));
            }

            @Override
            protected Builder self() {
                return this;
            }

            @Override
            public Metadata build() {
                return new Metadata();
            }
        }
    }

    /** Keeps base S3 bindings available to the SDK's metadata and signing services. */
    @ConfiguresHttpApi
    final class Module extends S3HttpApiModule<MinioS3Client> {
        public Module() {
            super(MinioS3Client.class);
        }

        @Override
        protected void configure() {
            super.configure();
            bind(S3Client.class).to(MinioS3Client.class);
            bind(new TypeLiteral<Class<S3Client>>() {}).toInstance(S3Client.class);
        }
    }
}
