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
import com.google.common.collect.Lists;
import org.jclouds.blobstore.BlobStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Jetty-specific handler for S3 requests.
 */
@Configuration
public class S3ProxyConfig {

    @Bean
    public S3ProxyHandler s3ProxyHandler(Environment properties) throws URISyntaxException {
        Builder builder = Builder.fromProperties(properties);
        S3ProxyHandler handler = new S3ProxyHandler(builder.blobStore,
                builder.authenticationType, builder.identity,
                builder.credential, builder.virtualHost,
                builder.v4MaxNonChunkedRequestSize,
                builder.ignoreUnknownHeaders, builder.corsRules,
                builder.servicePath, builder.maximumTimeSkew);
        return handler;
    }

    public static final class Builder {
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

            String endpoint = properties.getProperty(
                    S3ProxyConstants.PROPERTY_ENDPOINT);
            String secureEndpoint = properties.getProperty(
                    S3ProxyConstants.PROPERTY_SECURE_ENDPOINT);
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
            String authorizationString = properties.getProperty(
                    S3ProxyConstants.PROPERTY_AUTHORIZATION);
            if (authorizationString == null) {
                throw new IllegalArgumentException(
                        "Properties file must contain: " +
                                S3ProxyConstants.PROPERTY_AUTHORIZATION);
            }

            AuthenticationType authorization =
                    AuthenticationType.fromString(authorizationString);
            String localIdentity = null;
            String localCredential = null;
            switch (authorization) {
                case AWS_V2:
                case AWS_V4:
                case AWS_V2_OR_V4:
                    localIdentity = properties.getProperty(
                            S3ProxyConstants.PROPERTY_IDENTITY);
                    localCredential = properties.getProperty(
                            S3ProxyConstants.PROPERTY_CREDENTIAL);
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
                    throw new IllegalArgumentException(
                            S3ProxyConstants.PROPERTY_AUTHORIZATION +
                                    " invalid value, was: " + authorization);
            }

            if (localIdentity != null || localCredential != null) {
                builder.awsAuthentication(authorization, localIdentity,
                        localCredential);
            }

            String servicePath = Strings.nullToEmpty(properties.getProperty(
                    S3ProxyConstants.PROPERTY_SERVICE_PATH));
            if (servicePath != null) {
                builder.servicePath(servicePath);
            }

            String keyStorePath = properties.getProperty(
                    S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
            String keyStorePassword = properties.getProperty(
                    S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
            if (keyStorePath != null || keyStorePassword != null) {
                builder.keyStore(keyStorePath, keyStorePassword);
            }

            String virtualHost = properties.getProperty(
                    S3ProxyConstants.PROPERTY_VIRTUAL_HOST);
            if (!Strings.isNullOrEmpty(virtualHost)) {
                builder.virtualHost(virtualHost);
            }

            String v4MaxNonChunkedRequestSize = properties.getProperty(
                    S3ProxyConstants.PROPERTY_V4_MAX_NON_CHUNKED_REQUEST_SIZE);
            if (v4MaxNonChunkedRequestSize != null) {
                builder.v4MaxNonChunkedRequestSize(Long.parseLong(
                        v4MaxNonChunkedRequestSize));
            }

            String ignoreUnknownHeaders = properties.getProperty(
                    S3ProxyConstants.PROPERTY_IGNORE_UNKNOWN_HEADERS);
            if (!Strings.isNullOrEmpty(ignoreUnknownHeaders)) {
                builder.ignoreUnknownHeaders(Boolean.parseBoolean(
                        ignoreUnknownHeaders));
            }

            String corsAllowAll = properties.getProperty(
                    S3ProxyConstants.PROPERTY_CORS_ALLOW_ALL);
            if (!Strings.isNullOrEmpty(corsAllowAll) && Boolean.parseBoolean(
                    corsAllowAll)) {
                builder.corsRules(new CrossOriginResourceSharing());
            } else {
                String corsAllowOrigins = properties.getProperty(
                        S3ProxyConstants.PROPERTY_CORS_ALLOW_ORIGINS, "");
                String corsAllowMethods = properties.getProperty(
                        S3ProxyConstants.PROPERTY_CORS_ALLOW_METHODS, "");
                String corsAllowHeaders = properties.getProperty(
                        S3ProxyConstants.PROPERTY_CORS_ALLOW_HEADERS, "");
                Splitter splitter = Splitter.on(" ").trimResults()
                        .omitEmptyStrings();

                builder.corsRules(new CrossOriginResourceSharing(
                        Lists.newArrayList(splitter.split(corsAllowOrigins)),
                        Lists.newArrayList(splitter.split(corsAllowMethods)),
                        Lists.newArrayList(splitter.split(corsAllowHeaders))));
            }

            String jettyMaxThreads = properties.getProperty(
                    S3ProxyConstants.PROPERTY_JETTY_MAX_THREADS);
            if (jettyMaxThreads != null) {
                builder.jettyMaxThreads(Integer.parseInt(jettyMaxThreads));
            }

            String maximumTimeSkew = properties.getProperty(
                    S3ProxyConstants.PROPERTY_MAXIMUM_TIME_SKEW);
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
}
