/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.airavata.gfac.hadoop;

import org.apache.airavata.core.gfac.context.invocation.InvocationContext;
import org.apache.airavata.core.gfac.exception.GfacException;
import org.apache.airavata.core.gfac.exception.ProviderException;
import org.apache.airavata.core.gfac.provider.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @link{HadoopProvider} will execute map reduce jobs using hadoop
 */
public class HadoopProvider implements Provider {
    private static Logger log = LoggerFactory.getLogger(HadoopProvider.class);


    public void initialize(InvocationContext invocationContext) throws ProviderException {
        // Nothing to initialize at this stage. Assumes hadoop is already configured and required data
        // is there in HDFS.
        log.info("Nothing to intialize for hadoop job.");
    }

    public Map<String, ?> execute(InvocationContext invocationContext) throws ProviderException {

        return null;
    }

    public void dispose(InvocationContext invocationContext) throws GfacException {
        log.info("Nothing to dispose for hadoop job.");
    }


}
