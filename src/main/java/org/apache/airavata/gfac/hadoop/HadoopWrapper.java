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
import org.apache.airavata.registry.api.AiravataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopWrapper {
    private static final Logger log = LoggerFactory.getLogger(HadoopWrapper.class);
    private static String HADOOP_JAR_CMD = "hadoop jar %s %s %s %s";

    private InvocationContext invocationContext;
    private String jarUrl;
    private String mainClass;

    public HadoopWrapper(InvocationContext invocationContext){
        this.invocationContext = invocationContext;
    }

    public void runJar(){

    }

    private void moveJarToWorkingDir(){
        // TODO: Figure out a way to store jar file and move it to working directory
    }

    private String jarNameFromUrl(){
        return null;
    }

    /**
     * This method assumes executable name for hadoop job will follow the format
     * [jar-url]::[main-class]. If not exception will be thrown.
     * @param executableInfo executable name
     */
    private void parseExecutableString(String executableInfo) throws Exception {
        if(executableInfo != null){
            String[] parts = executableInfo.split("::");
            if (parts.length == 2){
                if(parts[0] != null){
                    jarUrl = parts[0];
                    if(!jarUrl.endsWith(".jar")){
                        throw new Exception("URL does not point to a jar.");
                    }
                } else {
                    throw new Exception("Jar URL null in executable info.");
                }

                if(parts[1] != null){
                    mainClass = parts[1];
                }
            } else {
                throw new Exception("Invalid executable info format. Required [jar-url]::[main-class].");
            }
        }else{
            throw new Exception("Executable info not available.");
        }
    }
}
