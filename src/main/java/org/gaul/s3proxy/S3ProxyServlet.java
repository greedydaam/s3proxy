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

import com.google.common.collect.ImmutableMap;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.http.HttpResponse;
import org.jclouds.http.HttpResponseException;
import org.jclouds.rest.AuthorizationException;
import org.jclouds.util.Throwables2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeoutException;

/**
 * tomcat-specific servlet for S3 requests.
 */
public class S3ProxyServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(
            S3ProxyServlet.class);

    private S3ProxyHandler handler;

    S3ProxyServlet(S3ProxyHandler handler) {
        this.handler = handler;
    }

    private void sendS3Exception(HttpServletRequest request,
                                 HttpServletResponse response, S3Exception se)
            throws IOException {
        handler.sendSimpleErrorResponse(request, response,
                se.getError(), se.getMessage(), se.getElements());
    }

    @Override
    public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try (InputStream is = request.getInputStream()) {
            // Set query encoding
            request.setAttribute(S3ProxyConstants.ATTRIBUTE_QUERY_ENCODING,
                    request.getCharacterEncoding());

            handler.doHandle(request, request, response, is);
        } catch (ContainerNotFoundException cnfe) {
            S3ErrorCode code = S3ErrorCode.NO_SUCH_BUCKET;
            handler.sendSimpleErrorResponse(request, response, code, code.getMessage(), ImmutableMap.<String, String>of());
        } catch (HttpResponseException hre) {
            HttpResponse hr = hre.getResponse();
            if (hr == null) {
                logger.debug("HttpResponseException without HttpResponse:",
                        hre);
                response.sendError(
                        HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        hre.getMessage());
                return;
            }
            int status = hr.getStatusCode();
            switch (status) {
                case 412:
                    sendS3Exception(request, response,
                            new S3Exception(S3ErrorCode.PRECONDITION_FAILED));
                    break;
                case 416:
                    sendS3Exception(request, response,
                            new S3Exception(S3ErrorCode.INVALID_RANGE));
                    break;
                case HttpServletResponse.SC_BAD_REQUEST:
                case 422:  // Swift returns 422 Unprocessable Entity
                    sendS3Exception(request, response,
                            new S3Exception(S3ErrorCode.BAD_DIGEST));
                    break;
                default:
                    response.sendError(status, hre.getContent());
                    break;
            }
        } catch (IllegalArgumentException iae) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    iae.getMessage());
        } catch (KeyNotFoundException knfe) {
            S3ErrorCode code = S3ErrorCode.NO_SUCH_KEY;
            handler.sendSimpleErrorResponse(request, response, code,
                    code.getMessage(), ImmutableMap.<String, String>of());
        } catch (S3Exception se) {
            sendS3Exception(request, response, se);
        } catch (UnsupportedOperationException uoe) {
            response.sendError(HttpServletResponse.SC_NOT_IMPLEMENTED,
                    uoe.getMessage());
        } catch (Throwable throwable) {
            if (Throwables2.getFirstThrowableOfType(throwable,
                    AuthorizationException.class) != null) {
                S3ErrorCode code = S3ErrorCode.ACCESS_DENIED;
                handler.sendSimpleErrorResponse(request, response, code,
                        code.getMessage(), ImmutableMap.<String, String>of());
            } else if (Throwables2.getFirstThrowableOfType(throwable,
                    TimeoutException.class) != null) {
                S3ErrorCode code = S3ErrorCode.REQUEST_TIMEOUT;
                handler.sendSimpleErrorResponse(request, response, code,
                        code.getMessage(), ImmutableMap.<String, String>of());
            } else {
                logger.debug("Unknown exception:", throwable);
                throw throwable;
            }
        }
    }

}
