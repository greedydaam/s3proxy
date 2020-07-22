/*
 * Copyright 2014-2020 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Module;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.JcloudsVersion;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.concurrent.DynamicExecutors;
import org.jclouds.concurrent.config.ExecutorServiceModule;
import org.jclouds.location.reference.LocationConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.swift.v1.blobstore.RegionScopedBlobStoreContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.origin.OriginTrackedValue;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Jetty-specific handler for S3 requests.
 */
@Configuration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = S3ProxyConstants.PROPERTY_SERVLET_ENABLED, havingValue = "true", matchIfMissing = true)
public class S3ProxyConfig {

    @Bean
    public ServletRegistrationBean s3ProxyHandler(Environment env) throws Exception {
        Builder builder = Builder.fromProperties(env);

        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat("s3p-thread %d")
                .setThreadFactory(Executors.defaultThreadFactory())
                .build();
        ExecutorService executorService = DynamicExecutors.newScalingThreadPool(1, 20, 60 * 1000, factory);

        ConfigProperties properties = new ConfigProperties();
        for (PropertySource<?> propertySource : ((AbstractEnvironment) env).getPropertySources()) {
            if (propertySource instanceof MapPropertySource) {
                properties.putAll(((MapPropertySource) propertySource).getSource());
            }
        }

        BlobStore blobStore = createBlobStore(properties, executorService);
        blobStore = parseMiddlewareProperties(blobStore, executorService, properties);

        S3ProxyHandler handler = new S3ProxyHandler(blobStore,
                builder.authenticationType, builder.identity,
                builder.credential, builder.virtualHost,
                builder.v4MaxNonChunkedRequestSize,
                builder.ignoreUnknownHeaders, builder.corsRules,
                builder.servicePath, builder.maximumTimeSkew);

        String s3ProxyAuthorizationString = env.getProperty(S3ProxyConstants.PROPERTY_AUTHORIZATION);
        if (AuthenticationType.fromString(s3ProxyAuthorizationString) != AuthenticationType.NONE) {
            ImmutableMap.Builder<String, Map.Entry<String, BlobStore>> locators = ImmutableMap.builder();

            String localIdentity = properties.getProperty(
                    S3ProxyConstants.PROPERTY_IDENTITY);
            String localCredential = properties.getProperty(
                    S3ProxyConstants.PROPERTY_CREDENTIAL);
            locators.put(localIdentity, Maps.immutableEntry(
                    localCredential, blobStore));
            final Map<String, Map.Entry<String, BlobStore>> locator = locators.build();
            if (!locator.isEmpty()) {
                handler.setBlobStoreLocator(new BlobStoreLocator() {
                    @Override
                    public Map.Entry<String, BlobStore> locateBlobStore(
                            String identity, String container, String blob) {
                        if (identity == null) {
                            if (locator.size() == 1) {
                                return locator.entrySet().iterator().next().getValue();
                            }
                            throw new IllegalArgumentException(
                                    "cannot use anonymous access with multiple" +
                                            " backends");
                        }
                        return locator.get(identity);
                    }
                });
            }
        }

        ServletRegistrationBean registrationBean = new ServletRegistrationBean();
        registrationBean.setServlet(new S3ProxyServlet(handler));
        registrationBean.addUrlMappings(builder.servicePath != null ? builder.servicePath + "/*" : "/s3proxy/*");
        return registrationBean;
    }

    private static final class Builder {
        private BlobStore blobStore;
        private URI endpoint;
        private URI secureEndpoint;
        private String servicePath;
        private AuthenticationType authenticationType =
                AuthenticationType.NONE;
        private String identity;
        private String credential;
        private String keyStorePath;
        private String keyStorePassword;
        private String virtualHost;
        private long v4MaxNonChunkedRequestSize = 32 * 1024 * 1024;
        private boolean ignoreUnknownHeaders;
        private CrossOriginResourceSharing corsRules;
        private int jettyMaxThreads = 200;  // sourced from QueuedThreadPool()
        private int maximumTimeSkew = 15 * 60;

        Builder() {
        }

        public static Builder fromProperties(Environment properties)
                throws URISyntaxException {
            Builder builder = new Builder();

            String endpoint = properties.getProperty(S3ProxyConstants.PROPERTY_ENDPOINT);
            String secureEndpoint = properties.getProperty(S3ProxyConstants.PROPERTY_SECURE_ENDPOINT);
            if (endpoint == null && secureEndpoint == null) {
                throw new IllegalArgumentException(
                        "Properties file must contain: " +
                                S3ProxyConstants.PROPERTY_ENDPOINT + " or " +
                                S3ProxyConstants.PROPERTY_SECURE_ENDPOINT);
            }
            if (endpoint != null) {
                builder.endpoint(new URI(endpoint));
            }
            if (secureEndpoint != null) {
                builder.secureEndpoint(new URI(secureEndpoint));
            }
            String authorizationString = properties.getProperty(S3ProxyConstants.PROPERTY_AUTHORIZATION);
            if (authorizationString == null) {
                throw new IllegalArgumentException(
                        "Properties file must contain: " +
                                S3ProxyConstants.PROPERTY_AUTHORIZATION);
            }

            AuthenticationType authorization = AuthenticationType.fromString(authorizationString);
            String localIdentity = null;
            String localCredential = null;
            switch (authorization) {
                case AWS_V2:
                case AWS_V4:
                case AWS_V2_OR_V4:
                    localIdentity = properties.getProperty(S3ProxyConstants.PROPERTY_IDENTITY);
                    localCredential = properties.getProperty(S3ProxyConstants.PROPERTY_CREDENTIAL);
                    if (localIdentity == null || localCredential == null) {
                        throw new IllegalArgumentException("Must specify both " +
                                S3ProxyConstants.PROPERTY_IDENTITY + " and " +
                                S3ProxyConstants.PROPERTY_CREDENTIAL +
                                " when using authentication");
                    }
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException(S3ProxyConstants.PROPERTY_AUTHORIZATION +
                            " invalid value, was: " + authorization);
            }

            if (localIdentity != null || localCredential != null) {
                builder.awsAuthentication(authorization, localIdentity, localCredential);
            }

            String servicePath = Strings.nullToEmpty(properties.getProperty(S3ProxyConstants.PROPERTY_SERVICE_PATH));
            if (servicePath != null) {
                builder.servicePath(servicePath);
            }

            String keyStorePath = properties.getProperty(S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
            String keyStorePassword = properties.getProperty(S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
            if (keyStorePath != null || keyStorePassword != null) {
                builder.keyStore(keyStorePath, keyStorePassword);
            }

            String virtualHost = properties.getProperty(S3ProxyConstants.PROPERTY_VIRTUAL_HOST);
            if (!Strings.isNullOrEmpty(virtualHost)) {
                builder.virtualHost(virtualHost);
            }

            String v4MaxNonChunkedRequestSize = properties.getProperty(S3ProxyConstants.PROPERTY_V4_MAX_NON_CHUNKED_REQUEST_SIZE);
            if (v4MaxNonChunkedRequestSize != null) {
                builder.v4MaxNonChunkedRequestSize(Long.parseLong(
                        v4MaxNonChunkedRequestSize));
            }

            String ignoreUnknownHeaders = properties.getProperty(S3ProxyConstants.PROPERTY_IGNORE_UNKNOWN_HEADERS);
            if (!Strings.isNullOrEmpty(ignoreUnknownHeaders)) {
                builder.ignoreUnknownHeaders(Boolean.parseBoolean(
                        ignoreUnknownHeaders));
            }

            String corsAllowAll = properties.getProperty(S3ProxyConstants.PROPERTY_CORS_ALLOW_ALL);
            if (!Strings.isNullOrEmpty(corsAllowAll) && Boolean.parseBoolean(
                    corsAllowAll)) {
                builder.corsRules(new CrossOriginResourceSharing());
            } else {
                String corsAllowOrigins = properties.getProperty(S3ProxyConstants.PROPERTY_CORS_ALLOW_ORIGINS, "");
                String corsAllowMethods = properties.getProperty(S3ProxyConstants.PROPERTY_CORS_ALLOW_METHODS, "");
                String corsAllowHeaders = properties.getProperty(S3ProxyConstants.PROPERTY_CORS_ALLOW_HEADERS, "");
                Splitter splitter = Splitter.on(" ").trimResults()
                        .omitEmptyStrings();

                builder.corsRules(new CrossOriginResourceSharing(
                        Lists.newArrayList(splitter.split(corsAllowOrigins)),
                        Lists.newArrayList(splitter.split(corsAllowMethods)),
                        Lists.newArrayList(splitter.split(corsAllowHeaders))));
            }

            String jettyMaxThreads = properties.getProperty(S3ProxyConstants.PROPERTY_JETTY_MAX_THREADS);
            if (jettyMaxThreads != null) {
                builder.jettyMaxThreads(Integer.parseInt(jettyMaxThreads));
            }

            String maximumTimeSkew = properties.getProperty(S3ProxyConstants.PROPERTY_MAXIMUM_TIME_SKEW);
            if (maximumTimeSkew != null) {
                builder.maximumTimeSkew(Integer.parseInt(maximumTimeSkew));
            }

            return builder;
        }

        public Builder blobStore(BlobStore blobStore) {
            this.blobStore = requireNonNull(blobStore);
            return this;
        }


        public Builder endpoint(URI endpoint) {
            this.endpoint = requireNonNull(endpoint);
            return this;
        }

        public Builder secureEndpoint(URI secureEndpoint) {
            this.secureEndpoint = requireNonNull(secureEndpoint);
            return this;
        }

        public Builder awsAuthentication(AuthenticationType authenticationType,
                                         String identity, String credential) {
            this.authenticationType = authenticationType;
            this.identity = requireNonNull(identity);
            this.credential = requireNonNull(credential);
            return this;
        }

        public Builder keyStore(String keyStorePath, String keyStorePassword) {
            this.keyStorePath = requireNonNull(keyStorePath);
            this.keyStorePassword = requireNonNull(keyStorePassword);
            return this;
        }

        public Builder virtualHost(String virtualHost) {
            this.virtualHost = requireNonNull(virtualHost);
            return this;
        }

        public Builder v4MaxNonChunkedRequestSize(
                long v4MaxNonChunkedRequestSize) {
            if (v4MaxNonChunkedRequestSize <= 0) {
                throw new IllegalArgumentException(
                        "must be greater than zero, was: " +
                                v4MaxNonChunkedRequestSize);
            }
            this.v4MaxNonChunkedRequestSize = v4MaxNonChunkedRequestSize;
            return this;
        }

        public Builder ignoreUnknownHeaders(boolean ignoreUnknownHeaders) {
            this.ignoreUnknownHeaders = ignoreUnknownHeaders;
            return this;
        }

        public Builder corsRules(CrossOriginResourceSharing corsRules) {
            this.corsRules = corsRules;
            return this;
        }

        public Builder jettyMaxThreads(int jettyMaxThreads) {
            this.jettyMaxThreads = jettyMaxThreads;
            return this;
        }

        public Builder maximumTimeSkew(int maximumTimeSkew) {
            this.maximumTimeSkew = maximumTimeSkew;
            return this;
        }

        public Builder servicePath(String s3ProxyServicePath) {
            String path = Strings.nullToEmpty(s3ProxyServicePath);

            if (!path.isEmpty()) {
                if (!path.startsWith("/")) {
                    path = "/" + path;
                }
            }

            this.servicePath = path;

            return this;
        }

        public URI getEndpoint() {
            return endpoint;
        }

        public URI getSecureEndpoint() {
            return secureEndpoint;
        }

        public String getServicePath() {
            return servicePath;
        }

        public String getIdentity() {
            return identity;
        }

        public String getCredential() {
            return credential;
        }

        private <T> T requireNonNull(T object) {
            if (object == null) {
                throw new IllegalArgumentException("object require");
            }
            return object;
        }
    }

    /**
     * 支持spring OriginTrackedValue
     */
    private static final class ConfigProperties extends Properties {
        public String getProperty(String key) {
            Object oval = super.get(key);
            /**spring OriginTrackedValue 支持**/
            if (oval instanceof OriginTrackedValue) {
                oval = ((OriginTrackedValue) oval).getValue();
            }
            String sval = (oval instanceof String) ? (String) oval : null;
            return ((sval == null) && (defaults != null)) ? defaults.getProperty(key) : sval;
        }
    }

    private static BlobStore createBlobStore(Properties properties, ExecutorService executorService) throws IOException {
        String provider = properties.getProperty(Constants.PROPERTY_PROVIDER);
        String identity = properties.getProperty(Constants.PROPERTY_IDENTITY);
        String credential = properties.getProperty(Constants.PROPERTY_CREDENTIAL);
        String endpoint = properties.getProperty(Constants.PROPERTY_ENDPOINT);
        properties.remove(Constants.PROPERTY_ENDPOINT);
        String region = properties.getProperty(LocationConstants.PROPERTY_REGION);

        if (provider == null) {
            System.err.println(
                    "Properties file must contain: " +
                            Constants.PROPERTY_PROVIDER);
            System.exit(1);
        }

        if (provider.equals("filesystem") || provider.equals("transient")) {
            // local blobstores do not require credentials
            identity = Strings.nullToEmpty(identity);
            credential = Strings.nullToEmpty(credential);
        } else if (provider.equals("google-cloud-storage")) {
            File credentialFile = new File(credential);
            if (credentialFile.exists()) {
                credential = Files.asCharSource(credentialFile,
                        StandardCharsets.UTF_8).read();
            }
            properties.remove(Constants.PROPERTY_CREDENTIAL);
        }

        if (identity == null || credential == null) {
            System.err.println(
                    "Properties file must contain: " +
                            Constants.PROPERTY_IDENTITY + " and " +
                            Constants.PROPERTY_CREDENTIAL);
            System.exit(1);
        }

        properties.setProperty(Constants.PROPERTY_USER_AGENT,
                String.format("s3proxy/%s jclouds/%s java/%s",
                        S3ProxyServlet.class.getPackage().getImplementationVersion(),
                        JcloudsVersion.get(),
                        System.getProperty("java.version")));

        ContextBuilder builder = ContextBuilder
                .newBuilder(provider)
                .credentials(identity, credential)
                .modules(ImmutableList.<Module>of(
                        new SLF4JLoggingModule(),
                        new ExecutorServiceModule(executorService)))
                .overrides(properties);
        if (!Strings.isNullOrEmpty(endpoint)) {
            builder = builder.endpoint(endpoint);
        }

        BlobStoreContext context = builder.build(BlobStoreContext.class);
        BlobStore blobStore;
        if (context instanceof RegionScopedBlobStoreContext && region != null) {
            blobStore = ((RegionScopedBlobStoreContext) context).getBlobStore(region);
        } else {
            blobStore = context.getBlobStore();
        }
        return blobStore;
    }

    private static BlobStore parseMiddlewareProperties(BlobStore blobStore,
                                                       ExecutorService executorService, Properties properties)
            throws IOException {
        Properties altProperties = new Properties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(S3ProxyConstants.PROPERTY_ALT_JCLOUDS_PREFIX)) {
                key = key.substring(
                        S3ProxyConstants.PROPERTY_ALT_JCLOUDS_PREFIX.length());
                altProperties.put(key, (String) entry.getValue());
            }
        }

        String eventualConsistency = properties.getProperty(S3ProxyConstants.PROPERTY_EVENTUAL_CONSISTENCY);
        if ("true".equalsIgnoreCase(eventualConsistency)) {
            BlobStore altBlobStore = createBlobStore(altProperties, executorService);
            int delay = Integer.parseInt(properties.getProperty(
                    S3ProxyConstants.PROPERTY_EVENTUAL_CONSISTENCY_DELAY, "5"));
            double probability = Double.parseDouble(properties.getProperty(
                    S3ProxyConstants.PROPERTY_EVENTUAL_CONSISTENCY_PROBABILITY, "1.0"));
            System.err.println("Emulating eventual consistency with delay " +
                    delay + " seconds and probability " + (probability * 100) +
                    "%");
            blobStore = EventualBlobStore.newEventualBlobStore(
                    blobStore, altBlobStore,
                    Executors.newScheduledThreadPool(1),
                    delay, TimeUnit.SECONDS, probability);
        }

        String nullBlobStore = properties.getProperty(S3ProxyConstants.PROPERTY_NULL_BLOBSTORE);
        if ("true".equalsIgnoreCase(nullBlobStore)) {
            System.err.println("Using null storage backend");
            blobStore = NullBlobStore.newNullBlobStore(blobStore);
        }

        String readOnlyBlobStore = properties.getProperty(S3ProxyConstants.PROPERTY_READ_ONLY_BLOBSTORE);
        if ("true".equalsIgnoreCase(readOnlyBlobStore)) {
            System.err.println("Using read-only storage backend");
            blobStore = ReadOnlyBlobStore.newReadOnlyBlobStore(blobStore);
        }

        return blobStore;
    }
}
