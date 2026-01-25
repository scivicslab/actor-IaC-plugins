/*
 * Copyright 2025 devteam@scivics-lab.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.scivicslab.actoriac.plugins.h2analyzer;

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.core.CallableByActionName;
import com.scivicslab.pojoactor.workflow.ActorSystemAware;
import com.scivicslab.pojoactor.workflow.IIActorRef;
import com.scivicslab.pojoactor.workflow.IIActorSystem;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.*;

/**
 * GPU Information Aggregator for actor-IaC workflows.
 *
 * <p>Analyzes H2 database logs and produces GPU information summaries.</p>
 *
 * <h2>Actions:</h2>
 * <ul>
 *   <li>connect - Connect to H2 database. Args: dbPath</li>
 *   <li>summarize-gpus - Summarize GPU info from logs. Args: sessionId (optional)</li>
 *   <li>disconnect - Disconnect from database</li>
 * </ul>
 *
 * @author devteam@scivics-lab.com
 * @since 1.0.0
 */
public class SystemInfoAggregator implements CallableByActionName, ActorSystemAware {

    private static final String CLASS_NAME = SystemInfoAggregator.class.getName();
    private static final Logger logger = Logger.getLogger(CLASS_NAME);

    private Connection connection;
    private String currentDbPath;
    private IIActorSystem system;

    @Override
    public void setActorSystem(IIActorSystem system) {
        logger.entering(CLASS_NAME, "setActorSystem", system);
        this.system = system;
        logger.exiting(CLASS_NAME, "setActorSystem");
    }

    @Override
    public ActionResult callByActionName(String actionName, String args) {
        logger.entering(CLASS_NAME, "callByActionName", new Object[]{actionName, args});
        try {
            ActionResult result = switch (actionName) {
                case "connect" -> connect(args);
                case "summarize-gpus" -> summarizeGpus(args);
                case "disconnect" -> disconnect();
                default -> new ActionResult(false, "Unknown action: " + actionName);
            };
            logger.exiting(CLASS_NAME, "callByActionName", result);
            return result;
        } catch (Exception e) {
            ActionResult errorResult = new ActionResult(false, "Error: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "callByActionName", "Exception occurred", e);
            logger.exiting(CLASS_NAME, "callByActionName", errorResult);
            return errorResult;
        }
    }

    /**
     * Connect to H2 database.
     *
     * @param args database path (JSON array ["path"] or plain string "path")
     */
    private ActionResult connect(String args) {
        logger.entering(CLASS_NAME, "connect", args);
        reportToMultiplexer("connect() called with: " + args);
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }

            // Parse argument - can be JSON array ["path"] or plain string "path"
            String dbPath;
            String trimmedArgs = args.trim();
            if (trimmedArgs.startsWith("[")) {
                JSONArray jsonArray = new JSONArray(trimmedArgs);
                if (jsonArray.length() < 1) {
                    return new ActionResult(false, "Invalid args. Expected: [dbPath]");
                }
                dbPath = jsonArray.getString(0).trim();
            } else {
                dbPath = trimmedArgs;
            }

            currentDbPath = dbPath;

            // Get JDBC URL from logServerApi actor
            String jdbcUrl = getJdbcUrlFromLogServerApi(dbPath);
            if (jdbcUrl == null) {
                // Fallback: use embedded mode if logServerApi is not available
                jdbcUrl = "jdbc:h2:" + dbPath + ";AUTO_SERVER=TRUE";
                logger.logp(Level.INFO, CLASS_NAME, "connect",
                    "logServerApi not available, using fallback URL: {0}", jdbcUrl);
            }

            // Use H2 Driver directly to avoid DriverManager classloader issues
            org.h2.Driver driver = new org.h2.Driver();
            connection = driver.connect(jdbcUrl, new java.util.Properties());

            if (connection == null) {
                ActionResult result = new ActionResult(false, "Connection returned null for: " + dbPath);
                logger.exiting(CLASS_NAME, "connect", result);
                return result;
            }

            logger.logp(Level.INFO, CLASS_NAME, "connect", "Connected to: {0}", jdbcUrl);
            ActionResult result = new ActionResult(true, "Connected to: " + dbPath);
            logger.exiting(CLASS_NAME, "connect", result);
            return result;
        } catch (SQLException e) {
            ActionResult result = new ActionResult(false, "Connection failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "connect", "SQLException occurred", e);
            logger.exiting(CLASS_NAME, "connect", result);
            return result;
        }
    }

    /**
     * Gets JDBC URL from logServerApi actor.
     */
    private String getJdbcUrlFromLogServerApi(String dbPath) {
        logger.entering(CLASS_NAME, "getJdbcUrlFromLogServerApi", dbPath);
        if (system == null) {
            logger.exiting(CLASS_NAME, "getJdbcUrlFromLogServerApi", null);
            return null;
        }

        IIActorRef<?> logServerApi = system.getIIActor("logServerApi");
        if (logServerApi == null) {
            logger.exiting(CLASS_NAME, "getJdbcUrlFromLogServerApi", null);
            return null;
        }

        ActionResult result = logServerApi.callByActionName("getJdbcUrl", dbPath);
        if (!result.isSuccess()) {
            logger.exiting(CLASS_NAME, "getJdbcUrlFromLogServerApi", null);
            return null;
        }

        String jdbcUrl = result.getResult();
        logger.logp(Level.INFO, CLASS_NAME, "getJdbcUrlFromLogServerApi",
            "Got JDBC URL from logServerApi: {0}", jdbcUrl);
        logger.exiting(CLASS_NAME, "getJdbcUrlFromLogServerApi", jdbcUrl);
        return jdbcUrl;
    }

    /**
     * Disconnect from database.
     */
    private ActionResult disconnect() {
        logger.entering(CLASS_NAME, "disconnect");
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                connection = null;
            }
            ActionResult result = new ActionResult(true, "Disconnected");
            logger.exiting(CLASS_NAME, "disconnect", result);
            return result;
        } catch (SQLException e) {
            ActionResult result = new ActionResult(false, "Disconnect failed: " + e.getMessage());
            logger.exiting(CLASS_NAME, "disconnect", result);
            return result;
        }
    }

    /**
     * Summarize GPU information from logs.
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeGpus(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeGpus", sessionIdStr);
        if (connection == null) {
            return new ActionResult(false, "Not connected. Use 'connect' first.");
        }

        try {
            long sessionId = resolveSessionId(sessionIdStr);
            String resultStr = buildGpuSummary(sessionId);
            if (resultStr == null) {
                resultStr = "No GPU information found in session " + sessionId;
            }
            reportToMultiplexer(resultStr);
            return new ActionResult(true, resultStr);
        } catch (Exception e) {
            return new ActionResult(false, "Query failed: " + e.getMessage());
        }
    }

    /**
     * Build GPU summary table.
     */
    private String buildGpuSummary(long sessionId) throws SQLException {
        // Get all messages from actors that have GPU INFO entries
        String sql = "SELECT actor_name, message FROM logs " +
                     "WHERE session_id = ? AND actor_name IN (" +
                     "  SELECT DISTINCT actor_name FROM logs " +
                     "  WHERE session_id = ? AND message LIKE '%GPU INFO%'" +
                     ") AND (message LIKE '%GPU INFO%' OR message LIKE '%NVIDIA%' " +
                     "OR message LIKE '%GeForce%' OR message LIKE '%Quadro%' " +
                     "OR message LIKE '%CUDA_VERSION%' OR message LIKE '%Radeon%' " +
                     "OR message LIKE '%AMD_GPU%' OR message LIKE '%ROCM_VERSION%' " +
                     "OR message LIKE '%GFX_ARCH%' OR message LIKE '%GPU_NAME%' " +
                     "OR message LIKE '%AMD%' OR message LIKE '%VGA%') " +
                     "ORDER BY actor_name, timestamp";

        Map<String, GpuInfo> nodeGpus = new LinkedHashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, sessionId);
            ps.setLong(2, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("actor_name");
                    String message = rs.getString("message");
                    GpuInfo gpuInfo = nodeGpus.computeIfAbsent(nodeId, k -> new GpuInfo());

                    for (String line : message.split("\n")) {
                        String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();
                        if (cleanLine.contains("GPU INFO") || cleanLine.isEmpty()) continue;

                        // Parse NVIDIA CUDA_VERSION line
                        if (cleanLine.startsWith("CUDA_VERSION:")) {
                            gpuInfo.toolkit = "CUDA " + cleanLine.replaceFirst("CUDA_VERSION:\\s*", "").trim();
                            continue;
                        }

                        // Parse AMD ROCm output
                        if (cleanLine.startsWith("AMD_GPU:")) {
                            gpuInfo.isAmd = true;
                            continue;
                        }
                        if (cleanLine.startsWith("GPU_NAME:")) {
                            gpuInfo.name = cleanLine.replaceFirst("GPU_NAME:\\s*", "").trim();
                            continue;
                        }
                        if (cleanLine.startsWith("VRAM_BYTES:")) {
                            try {
                                long vramBytes = Long.parseLong(cleanLine.replaceFirst("VRAM_BYTES:\\s*", "").trim());
                                long vramGB = vramBytes / (1024L * 1024L * 1024L);
                                gpuInfo.vram = vramGB + "GB";
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                            continue;
                        }
                        if (cleanLine.startsWith("DRIVER_VERSION:")) {
                            gpuInfo.driver = cleanLine.replaceFirst("DRIVER_VERSION:\\s*", "").trim();
                            continue;
                        }
                        if (cleanLine.startsWith("ROCM_VERSION:")) {
                            gpuInfo.toolkit = "ROCm " + cleanLine.replaceFirst("ROCM_VERSION:\\s*", "").trim();
                            continue;
                        }
                        if (cleanLine.startsWith("GFX_ARCH:")) {
                            gpuInfo.arch = cleanLine.replaceFirst("GFX_ARCH:\\s*", "").trim();
                            continue;
                        }

                        // Parse nvidia-smi CSV output: name, memory.total, driver_version, compute_cap
                        Pattern nvidiaCsvPattern = Pattern.compile(
                            "^(NVIDIA [^,]+|[^,]*GeForce[^,]*|[^,]*Quadro[^,]*|[^,]*Tesla[^,]*|[^,]*A100[^,]*|[^,]*H100[^,]*|[^,]*GB[0-9]+[^,]*),\\s*(\\d+)\\s*MiB,\\s*([\\d.]+),\\s*([\\d.]+)$"
                        );
                        Matcher nvidiaMatcher = nvidiaCsvPattern.matcher(cleanLine);
                        if (nvidiaMatcher.find()) {
                            gpuInfo.name = nvidiaMatcher.group(1).trim();
                            int vramMB = Integer.parseInt(nvidiaMatcher.group(2));
                            gpuInfo.vram = (vramMB >= 1024) ? (vramMB / 1024) + "GB" : vramMB + "MB";
                            gpuInfo.driver = nvidiaMatcher.group(3).trim();
                            gpuInfo.arch = nvidiaMatcher.group(4).trim();
                            continue;
                        }

                        // Parse lspci output for AMD/Intel GPUs (fallback when no ROCm)
                        Pattern lspciPattern = Pattern.compile(
                            "(?:VGA compatible controller|3D controller|Display controller):\\s*(.+?)(?:\\s*\\(rev|$)");
                        Matcher lspciMatcher = lspciPattern.matcher(cleanLine);
                        if (lspciMatcher.find()) {
                            String gpuName = lspciMatcher.group(1).trim();
                            if (gpuInfo.name == null) {
                                gpuInfo.name = gpuName;
                                gpuInfo.driver = "-";
                                gpuInfo.vram = "-";
                                gpuInfo.arch = "-";
                                gpuInfo.toolkit = "-";
                            }
                        }
                    }
                }
            }
        }

        if (nodeGpus.isEmpty()) return null;

        // Count GPU types
        int nvidiaCount = 0, amdCount = 0, otherCount = 0;
        for (GpuInfo gpu : nodeGpus.values()) {
            if (gpu.name != null) {
                if (gpu.name.contains("NVIDIA") || gpu.name.contains("GeForce") ||
                    gpu.name.contains("Quadro") || gpu.name.contains("Tesla")) {
                    nvidiaCount++;
                } else if (gpu.isAmd || gpu.name.contains("AMD") || gpu.name.contains("Radeon")) {
                    amdCount++;
                } else {
                    otherCount++;
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("## GPU Summary\n");
        sb.append("| node | gpu | vram | driver | toolkit | arch |\n");
        sb.append("|------|-----|------|--------|---------|------|\n");
        for (Map.Entry<String, GpuInfo> entry : nodeGpus.entrySet()) {
            String nodeShort = entry.getKey().replaceFirst("^node-", "");
            GpuInfo gpu = entry.getValue();
            if (gpu.name != null) {
                sb.append(String.format("| %s | %s | %s | %s | %s | %s |%n",
                        nodeShort,
                        gpu.name,
                        gpu.vram != null ? gpu.vram : "-",
                        gpu.driver != null ? gpu.driver : "-",
                        gpu.toolkit != null ? gpu.toolkit : "-",
                        gpu.arch != null ? gpu.arch : "-"));
            }
        }

        sb.append("\nSummary: ");
        if (nvidiaCount > 0) sb.append(nvidiaCount).append(" NVIDIA");
        if (amdCount > 0) {
            if (nvidiaCount > 0) sb.append(", ");
            sb.append(amdCount).append(" AMD");
        }
        if (otherCount > 0) {
            if (nvidiaCount > 0 || amdCount > 0) sb.append(", ");
            sb.append(otherCount).append(" Other");
        }

        return sb.toString();
    }

    /**
     * Report result to outputMultiplexer.
     */
    private void reportToMultiplexer(String data) {
        if (system == null) {
            throw new IllegalStateException("ActorSystem not injected");
        }

        IIActorRef<?> multiplexer = system.getIIActor("outputMultiplexer");
        if (multiplexer == null) {
            throw new IllegalStateException("outputMultiplexer actor not found");
        }

        JSONObject arg = new JSONObject();
        arg.put("source", "system-info-aggregator");
        arg.put("type", "report");  // shown in --report-only mode
        arg.put("data", data);
        ActionResult result = multiplexer.callByActionName("add", arg.toString());
        if (!result.isSuccess()) {
            throw new IllegalStateException("outputMultiplexer.add() failed: " + result.getResult());
        }
    }

    /**
     * Retrieves session ID from nodeGroup actor.
     */
    private String getSessionIdFromNodeGroup() {
        if (system == null) return null;
        IIActorRef<?> nodeGroup = system.getIIActor("nodeGroup");
        if (nodeGroup == null) return null;
        ActionResult result = nodeGroup.callByActionName("getSessionId", "");
        return result.isSuccess() ? result.getResult() : null;
    }

    /**
     * Resolve session ID from argument or auto-retrieve from nodeGroup.
     */
    private long resolveSessionId(String sessionIdStr) throws NumberFormatException {
        if (sessionIdStr == null || sessionIdStr.trim().isEmpty() || sessionIdStr.equals("[]")) {
            String autoSessionId = getSessionIdFromNodeGroup();
            if (autoSessionId == null) {
                throw new NumberFormatException("Session ID not specified and could not retrieve from nodeGroup");
            }
            return Long.parseLong(autoSessionId);
        }
        return Long.parseLong(sessionIdStr.trim());
    }

    /**
     * GPU information holder.
     */
    private static class GpuInfo {
        String name;
        String vram;
        String driver;
        String toolkit;  // CUDA x.x or ROCm x.x
        String arch;     // compute cap (NVIDIA) or gfx ID (AMD)
        boolean isAmd;
    }
}
