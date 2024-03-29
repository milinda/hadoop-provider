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
import org.apache.airavata.schemas.gfac.ApplicationDeploymentDescriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class HadoopWrapper {
    private static final Logger log = LoggerFactory.getLogger(HadoopWrapper.class);
    private static String HADOOP_JAR_CMD = "hadoop jar %s %s %s %s";

    private InvocationContext invocationContext;
    private String jarUrl;
    private String mainClass;

    public HadoopWrapper(InvocationContext invocationContext) {
        this.invocationContext = invocationContext;
    }

    public void runJar(String stdOutFile, String stdErrFile) throws InterruptedException, IOException {
        ApplicationDeploymentDescriptionType appDesc = invocationContext.getExecutionDescription().getApp().getType();
        String inputDataDir = appDesc.getInputDataDirectory();
        String outputDataDir = appDesc.getOutputDataDirectory();

        String hadoopJarCmd = String.format(HADOOP_JAR_CMD, jarNameFromUrl(jarUrl), mainClass, inputDataDir, outputDataDir);
        try {
            Process proc = Runtime.getRuntime().exec(hadoopJarCmd);

            Thread stdOutT = new ReadStreamWriteFile(proc.getInputStream(), stdOutFile);
            Thread stdErrT = new ReadStreamWriteFile(proc.getErrorStream(), stdErrFile);

            stdOutT.setDaemon(true);
            stdErrT.setDaemon(true);
            stdOutT.start();
            stdErrT.start();

            int procReturn = proc.waitFor();

            stdOutT.join();
            stdErrT.join();

            /*
            * check return value. usually not very helpful to draw conclusions based on return values so don't bother.
            * just provide warning in the log messages
            */
            if (procReturn != 0) {
                log.warn("Hadoop jar command finished with non zero return value. Hadoop job may have failed");
            }

            StringBuffer buf = new StringBuffer();
            buf.append("Executed ").append(hadoopJarCmd)
                    .append(" on the localHost, working directory = ").append(appDesc.getStaticWorkingDirectory())
                    .append(" tempDirectory = ").append(appDesc.getScratchWorkingDirectory()).append(" With the status ")
                    .append(String.valueOf(procReturn));

            log.info(buf.toString());
        } catch (InterruptedException e) {
            log.error("Hadoop job interrupted!!", e);
            throw e;
        } catch (IOException e) {
            log.error("IO error during hadoop jar execution!!", e);
            throw e;
        }
    }

    private void moveJarToWorkingDir() {
        // TODO: Figure out a way to store jar file and move it to working directory
    }

    private static String jarNameFromUrl(String jarUrl) {
        String[] pathParts = jarUrl.split("/");
        return pathParts[pathParts.length - 1];
    }

    /**
     * This method assumes executable name for hadoop job will follow the format
     * [jar-url]::[main-class]. If not exception will be thrown.
     *
     * @param executableInfo executable name
     */
    private void parseExecutableString(String executableInfo) throws Exception {
        if (executableInfo != null) {
            String[] parts = executableInfo.split("::");
            if (parts.length == 2) {
                if (parts[0] != null) {
                    jarUrl = parts[0];
                    if (!jarUrl.endsWith(".jar")) {
                        throw new Exception("URL does not point to a jar.");
                    }
                } else {
                    throw new Exception("Jar URL null in executable info.");
                }

                if (parts[1] != null) {
                    mainClass = parts[1];
                }
            } else {
                throw new Exception("Invalid executable info format. Required [jar-url]::[main-class].");
            }
        } else {
            throw new Exception("Executable info not available.");
        }
    }

    private class ReadStreamWriteFile extends Thread {
        private BufferedReader in;
        private BufferedWriter out;

        public ReadStreamWriteFile(InputStream in, String out) throws IOException {
            this.in = new BufferedReader(new InputStreamReader(in));
            this.out = new BufferedWriter(new FileWriter(out));
        }

        public void run() {
            try {
                String line = null;
                while ((line = in.readLine()) != null) {
                    log.debug(line);
                    out.write(line);
                    out.newLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (out != null) {
                    try {
                        out.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
