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
 * System Information Aggregator for actor-IaC workflows.
 *
 * <p>Analyzes H2 database logs and produces system information summaries.</p>
 *
 * <h2>Actions:</h2>
 * <ul>
 *   <li>connect - Connect to H2 database. Args: dbPath</li>
 *   <li>summarize-disks - Summarize disk info from logs. Args: sessionId</li>
 *   <li>summarize-cpu - Summarize CPU info from logs. Args: sessionId</li>
 *   <li>summarize-gpu - Summarize GPU info from logs. Args: sessionId</li>
 *   <li>summarize-memory - Summarize memory info from logs. Args: sessionId</li>
 *   <li>summarize-network - Summarize network info from logs. Args: sessionId</li>
 *   <li>summarize-all - Summarize all system info. Args: sessionId</li>
 *   <li>list-sessions - List available sessions</li>
 *   <li>node-status - Show node status summary. Args: sessionId</li>
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
                case "summarize-disks" -> summarizeDisks(args);
                case "summarize-cpu" -> summarizeCpu(args);
                case "summarize-gpu" -> summarizeGpu(args);
                case "summarize-memory" -> summarizeMemory(args);
                case "summarize-network" -> summarizeNetwork(args);
                case "summarize-all" -> summarizeAll(args);
                case "list-sessions" -> listSessions();
                case "node-status" -> nodeStatus(args);
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
     * <p>Connection strategy:</p>
     * <ol>
     *   <li>Query logServerApi actor to discover the correct log server port</li>
     *   <li>Use the returned JDBC URL (TCP if server found, embedded otherwise)</li>
     * </ol>
     *
     * <p>This design ensures the plugin connects to the correct database server
     * even when multiple actor-IaC instances are running on different ports.</p>
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
            // This actor discovers the correct log server port automatically
            String jdbcUrl = getJdbcUrlFromLogServerApi(dbPath);
            if (jdbcUrl == null) {
                // Fallback: use embedded mode if logServerApi is not available
                jdbcUrl = "jdbc:h2:" + dbPath + ";AUTO_SERVER=TRUE";
                logger.logp(Level.INFO, CLASS_NAME, "connect",
                    "logServerApi not available, using fallback URL: {0}", jdbcUrl);
            }

            // Use H2 Driver directly to avoid DriverManager classloader issues
            // when this plugin is loaded via URLClassLoader
            org.h2.Driver driver = new org.h2.Driver();
            connection = driver.connect(jdbcUrl, new java.util.Properties());

            if (connection == null) {
                ActionResult result = new ActionResult(false, "Connection returned null for: " + dbPath);
                logger.exiting(CLASS_NAME, "connect", result);
                return result;
            }

            logger.logp(Level.INFO, CLASS_NAME, "connect",
                "Connected to: {0}", jdbcUrl);
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
     *
     * <p>The logServerApi actor discovers running log servers and returns
     * the appropriate JDBC URL (TCP mode if server found, embedded mode otherwise).</p>
     *
     * @param dbPath the database path
     * @return JDBC URL string, or null if logServerApi is not available
     */
    private String getJdbcUrlFromLogServerApi(String dbPath) {
        logger.entering(CLASS_NAME, "getJdbcUrlFromLogServerApi", dbPath);
        if (system == null) {
            logger.logp(Level.FINE, CLASS_NAME, "getJdbcUrlFromLogServerApi", "system is null");
            logger.exiting(CLASS_NAME, "getJdbcUrlFromLogServerApi", null);
            return null;
        }

        IIActorRef<?> logServerApi = system.getIIActor("logServerApi");
        if (logServerApi == null) {
            logger.logp(Level.FINE, CLASS_NAME, "getJdbcUrlFromLogServerApi",
                "logServerApi actor not found");
            logger.exiting(CLASS_NAME, "getJdbcUrlFromLogServerApi", null);
            return null;
        }

        ActionResult result = logServerApi.callByActionName("getJdbcUrl", dbPath);
        if (!result.isSuccess()) {
            logger.logp(Level.WARNING, CLASS_NAME, "getJdbcUrlFromLogServerApi",
                "logServerApi.getJdbcUrl failed: {0}", result.getResult());
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
            logger.logp(Level.WARNING, CLASS_NAME, "disconnect", "SQLException occurred", e);
            logger.exiting(CLASS_NAME, "disconnect", result);
            return result;
        }
    }

    /**
     * List available sessions.
     */
    private ActionResult listSessions() {
        logger.entering(CLASS_NAME, "listSessions");
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "listSessions", result);
            return result;
        }

        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Sessions:\n");

            String sql = "SELECT id, workflow_name, status, started_at FROM sessions ORDER BY id DESC LIMIT 10";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                while (rs.next()) {
                    sb.append(String.format("#%-4d %-30s %-10s %s%n",
                            rs.getLong("id"),
                            rs.getString("workflow_name"),
                            rs.getString("status"),
                            rs.getTimestamp("started_at")));
                }
            }

            String resultStr = sb.toString();
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "listSessions", result);
            return result;
        } catch (SQLException e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "listSessions", "SQLException occurred", e);
            logger.exiting(CLASS_NAME, "listSessions", result);
            return result;
        }
    }

    /**
     * Summarize disk information from logs.
     *
     * <p>If sessionIdStr is empty or "[]", automatically retrieves session ID
     * from nodeGroup actor.</p>
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeDisks(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeDisks", sessionIdStr);
        reportToMultiplexer("summarizeDisks() called with: " + sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "summarizeDisks", result);
            return result;
        }

        try {
            long sessionId;

            // Auto-retrieve session ID from nodeGroup if not specified
            if (sessionIdStr == null || sessionIdStr.trim().isEmpty() || sessionIdStr.equals("[]")) {
                String autoSessionId = getSessionIdFromNodeGroup();
                if (autoSessionId == null) {
                    ActionResult result = new ActionResult(false,
                        "Session ID not specified and could not retrieve from nodeGroup");
                    logger.exiting(CLASS_NAME, "summarizeDisks", result);
                    return result;
                }
                sessionId = Long.parseLong(autoSessionId);
            } else {
                sessionId = Long.parseLong(sessionIdStr.trim());
            }

            logger.logp(Level.FINER, CLASS_NAME, "summarizeDisks", "Using sessionId: {0}", sessionId);

            // Extract disk-related log entries
            String sql = "SELECT node_id, message FROM logs " +
                         "WHERE session_id = ? AND message LIKE '%DISK INFO%' " +
                         "ORDER BY node_id, timestamp";

            Map<String, List<DiskInfo>> nodeDisks = new LinkedHashMap<>();

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id");
                        String message = rs.getString("message");

                        // Parse each line in the multi-line message
                        for (String line : message.split("\n")) {
                            // Strip node prefix like [node-192.168.5.13]
                            String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();

                            // Parse disk info from lsblk output
                            // Pattern: sda 1.9T disk TS2TSSD230S
                            //          nvme0n1 931.5G disk CSSD-M2B1TPG3NF2
                            Pattern diskPattern = Pattern.compile(
                                "(sd[a-z]+|nvme\\d+n\\d+)\\s+([\\d.]+[GMTP])\\s+disk\\s+(.+)$"
                            );
                            Matcher m = diskPattern.matcher(cleanLine);
                            if (m.find()) {
                                DiskInfo disk = new DiskInfo(m.group(1), m.group(2), m.group(3).trim());
                                nodeDisks.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(disk);
                            }
                        }
                    }
                }
            }

            if (nodeDisks.isEmpty()) {
                String resultStr = "No disk information found in session " + sessionId;
                reportToMultiplexer(resultStr);
                ActionResult result = new ActionResult(true, resultStr);
                logger.exiting(CLASS_NAME, "summarizeDisks", result);
                return result;
            }

            // Build markdown table
            StringBuilder sb = new StringBuilder();
            sb.append("| node | disk | model | size |\n");
            sb.append("|------|------|-------|------|\n");

            for (Map.Entry<String, List<DiskInfo>> entry : nodeDisks.entrySet()) {
                String nodeId = entry.getKey();
                // Extract just the IP part from node-192.168.5.13
                String nodeShort = nodeId.replaceFirst("^node-", "");

                for (DiskInfo disk : entry.getValue()) {
                    sb.append(String.format("| %s | %s | %s | %s |%n",
                            nodeShort, disk.device, disk.model, disk.size));
                }
            }

            String resultStr = sb.toString();
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "summarizeDisks", result);
            return result;

        } catch (NumberFormatException e) {
            ActionResult result = new ActionResult(false, "Invalid session ID: " + sessionIdStr);
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeDisks", "NumberFormatException occurred", e);
            logger.exiting(CLASS_NAME, "summarizeDisks", result);
            return result;
        } catch (SQLException e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeDisks", "SQLException occurred", e);
            logger.exiting(CLASS_NAME, "summarizeDisks", result);
            return result;
        }
    }

    /**
     * Show node status summary.
     *
     * <p>If sessionIdStr is empty or "[]", automatically retrieves session ID
     * from nodeGroup actor.</p>
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult nodeStatus(String sessionIdStr) {
        logger.entering(CLASS_NAME, "nodeStatus", sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "nodeStatus", result);
            return result;
        }

        try {
            long sessionId;

            // Auto-retrieve session ID from nodeGroup if not specified
            if (sessionIdStr == null || sessionIdStr.trim().isEmpty() || sessionIdStr.equals("[]")) {
                String autoSessionId = getSessionIdFromNodeGroup();
                if (autoSessionId == null) {
                    ActionResult result = new ActionResult(false,
                        "Session ID not specified and could not retrieve from nodeGroup");
                    logger.exiting(CLASS_NAME, "nodeStatus", result);
                    return result;
                }
                sessionId = Long.parseLong(autoSessionId);
            } else {
                sessionId = Long.parseLong(sessionIdStr.trim());
            }

            logger.logp(Level.FINER, CLASS_NAME, "nodeStatus", "Using sessionId: {0}", sessionId);

            StringBuilder sb = new StringBuilder();
            sb.append("Node Status (Session #").append(sessionId).append("):\n");
            sb.append("| node | status | log_lines |\n");
            sb.append("|------|--------|----------|\n");

            String sql = """
                SELECT nr.node_id, nr.status, COUNT(l.id) as log_count
                FROM node_results nr
                LEFT JOIN logs l ON nr.session_id = l.session_id AND nr.node_id = l.node_id
                WHERE nr.session_id = ?
                GROUP BY nr.node_id, nr.status
                ORDER BY nr.node_id
                """;

            int successCount = 0;
            int failedCount = 0;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id").replaceFirst("^node-", "");
                        String status = rs.getString("status");
                        int logCount = rs.getInt("log_count");

                        sb.append(String.format("| %s | %s | %d |%n", nodeId, status, logCount));

                        if ("SUCCESS".equals(status)) {
                            successCount++;
                        } else {
                            failedCount++;
                        }
                    }
                }
            }

            sb.append("\nSummary: ").append(successCount).append(" SUCCESS, ")
              .append(failedCount).append(" FAILED");

            String resultStr = sb.toString();
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "nodeStatus", result);
            return result;

        } catch (NumberFormatException e) {
            ActionResult result = new ActionResult(false, "Invalid session ID: " + sessionIdStr);
            logger.logp(Level.WARNING, CLASS_NAME, "nodeStatus", "NumberFormatException occurred", e);
            logger.exiting(CLASS_NAME, "nodeStatus", result);
            return result;
        } catch (SQLException e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "nodeStatus", "SQLException occurred", e);
            logger.exiting(CLASS_NAME, "nodeStatus", result);
            return result;
        }
    }

    /**
     * Report result to outputMultiplexer for console/file/database logging.
     *
     * <p>This is the plugin equivalent of NodeIIAR.reportToAccumulator().
     * Uses loose-coupled actor communication via actor name.</p>
     *
     * @param data the result data to output
     */
    private void reportToMultiplexer(String data) {
        logger.entering(CLASS_NAME, "reportToMultiplexer");
        if (system == null) {
            var e = new IllegalStateException("ActorSystem not injected - setActorSystem() was not called");
            logger.throwing(CLASS_NAME, "reportToMultiplexer", e);
            throw e;
        }

        IIActorRef<?> multiplexer = system.getIIActor("outputMultiplexer");
        if (multiplexer == null) {
            var e = new IllegalStateException("outputMultiplexer actor not found in ActorSystem");
            logger.throwing(CLASS_NAME, "reportToMultiplexer", e);
            throw e;
        }

        JSONObject arg = new JSONObject();
        arg.put("source", "system-info-aggregator");
        arg.put("type", "report");  // shown in --report-only mode
        arg.put("data", data);
        ActionResult result = multiplexer.callByActionName("add", arg.toString());
        if (!result.isSuccess()) {
            var e = new IllegalStateException("outputMultiplexer.add() failed: " + result.getResult());
            logger.throwing(CLASS_NAME, "reportToMultiplexer", e);
            throw e;
        }
        logger.exiting(CLASS_NAME, "reportToMultiplexer");
    }

    /**
     * Retrieves session ID from nodeGroup actor.
     *
     * @return session ID string, or null if not available
     */
    private String getSessionIdFromNodeGroup() {
        logger.entering(CLASS_NAME, "getSessionIdFromNodeGroup");
        if (system == null) {
            logger.logp(Level.FINER, CLASS_NAME, "getSessionIdFromNodeGroup", "system is null");
            logger.exiting(CLASS_NAME, "getSessionIdFromNodeGroup", null);
            return null;
        }
        IIActorRef<?> nodeGroup = system.getIIActor("nodeGroup");
        if (nodeGroup == null) {
            logger.logp(Level.FINER, CLASS_NAME, "getSessionIdFromNodeGroup", "nodeGroup actor not found");
            logger.exiting(CLASS_NAME, "getSessionIdFromNodeGroup", null);
            return null;
        }
        ActionResult result = nodeGroup.callByActionName("getSessionId", "");
        String sessionId = result.isSuccess() ? result.getResult() : null;
        logger.exiting(CLASS_NAME, "getSessionIdFromNodeGroup", sessionId);
        return sessionId;
    }

    /**
     * Summarize CPU information from logs.
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeCpu(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeCpu", sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "summarizeCpu", result);
            return result;
        }

        try {
            long sessionId = resolveSessionId(sessionIdStr);
            String resultStr = buildCpuSummary(sessionId);
            if (resultStr == null) {
                resultStr = "No CPU information found in session " + sessionId;
            }
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "summarizeCpu", result);
            return result;
        } catch (Exception e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeCpu", "Exception occurred", e);
            logger.exiting(CLASS_NAME, "summarizeCpu", result);
            return result;
        }
    }

    /**
     * Summarize GPU information from logs.
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeGpu(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeGpu", sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "summarizeGpu", result);
            return result;
        }

        try {
            long sessionId = resolveSessionId(sessionIdStr);
            String resultStr = buildGpuSummary(sessionId);
            if (resultStr == null) {
                resultStr = "No GPU information found in session " + sessionId;
            }
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "summarizeGpu", result);
            return result;
        } catch (Exception e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeGpu", "Exception occurred", e);
            logger.exiting(CLASS_NAME, "summarizeGpu", result);
            return result;
        }
    }

    /**
     * Summarize memory information from logs.
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeMemory(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeMemory", sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "summarizeMemory", result);
            return result;
        }

        try {
            long sessionId = resolveSessionId(sessionIdStr);
            String resultStr = buildMemorySummary(sessionId);
            if (resultStr == null) {
                resultStr = "No memory information found in session " + sessionId;
            }
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "summarizeMemory", result);
            return result;
        } catch (Exception e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeMemory", "Exception occurred", e);
            logger.exiting(CLASS_NAME, "summarizeMemory", result);
            return result;
        }
    }

    /**
     * Summarize network information from logs.
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeNetwork(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeNetwork", sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "summarizeNetwork", result);
            return result;
        }

        try {
            long sessionId = resolveSessionId(sessionIdStr);
            String resultStr = buildNetworkSummary(sessionId);
            if (resultStr == null) {
                resultStr = "No network information found in session " + sessionId;
            }
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "summarizeNetwork", result);
            return result;
        } catch (Exception e) {
            ActionResult result = new ActionResult(false, "Query failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeNetwork", "Exception occurred", e);
            logger.exiting(CLASS_NAME, "summarizeNetwork", result);
            return result;
        }
    }

    /**
     * Summarize all system information from logs.
     *
     * @param sessionIdStr session ID to analyze, or empty for auto-retrieval
     */
    private ActionResult summarizeAll(String sessionIdStr) {
        logger.entering(CLASS_NAME, "summarizeAll", sessionIdStr);
        if (connection == null) {
            ActionResult result = new ActionResult(false, "Not connected. Use 'connect' first.");
            logger.exiting(CLASS_NAME, "summarizeAll", result);
            return result;
        }

        try {
            long sessionId = resolveSessionId(sessionIdStr);

            StringBuilder sb = new StringBuilder();
            sb.append("# System Information Summary (Session #").append(sessionId).append(")\n\n");

            // Collect all summaries using internal methods (no reporting)
            String cpuResult = buildCpuSummary(sessionId);
            if (cpuResult != null) {
                sb.append(cpuResult).append("\n");
            }

            String gpuResult = buildGpuSummary(sessionId);
            if (gpuResult != null) {
                sb.append(gpuResult).append("\n");
            }

            String memResult = buildMemorySummary(sessionId);
            if (memResult != null) {
                sb.append(memResult).append("\n");
            }

            String diskResult = buildDiskSummary(sessionId);
            if (diskResult != null) {
                sb.append("## Disk Summary\n");
                sb.append(diskResult).append("\n");
            }

            String netResult = buildNetworkSummary(sessionId);
            if (netResult != null) {
                sb.append(netResult).append("\n");
            }

            String resultStr = sb.toString();
            reportToMultiplexer(resultStr);
            ActionResult result = new ActionResult(true, resultStr);
            logger.exiting(CLASS_NAME, "summarizeAll", result);
            return result;

        } catch (Exception e) {
            ActionResult result = new ActionResult(false, "Summarize failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "summarizeAll", "Exception occurred", e);
            logger.exiting(CLASS_NAME, "summarizeAll", result);
            return result;
        }
    }

    // ========== Internal build methods (no reporting) ==========

    /**
     * Build CPU summary table without reporting.
     * @return markdown table or null if no data
     */
    private String buildCpuSummary(long sessionId) throws SQLException {
        String sql = "SELECT node_id, message FROM logs " +
                     "WHERE session_id = ? AND message LIKE '%CPU INFO%' " +
                     "ORDER BY node_id, timestamp";

        Map<String, CpuInfo> nodeCpus = new LinkedHashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("node_id");
                    String message = rs.getString("message");
                    CpuInfo cpu = nodeCpus.computeIfAbsent(nodeId, k -> new CpuInfo());
                    for (String line : message.split("\n")) {
                        String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();
                        if (cleanLine.startsWith("Model name:")) {
                            cpu.model = cleanLine.replaceFirst("Model name:\\s*", "").trim();
                        } else if (cleanLine.startsWith("CPU(s):")) {
                            Matcher m = Pattern.compile("CPU\\(s\\):\\s*(\\d+)").matcher(cleanLine);
                            if (m.find()) cpu.cores = m.group(1);
                        } else if (cleanLine.startsWith("Architecture:")) {
                            cpu.arch = cleanLine.replaceFirst("Architecture:\\s*", "").trim();
                        }
                    }
                }
            }
        }

        if (nodeCpus.isEmpty()) return null;

        StringBuilder sb = new StringBuilder();
        sb.append("## CPU Summary\n");
        sb.append("| node | model | cores | arch |\n");
        sb.append("|------|-------|-------|------|\n");
        for (Map.Entry<String, CpuInfo> entry : nodeCpus.entrySet()) {
            String nodeShort = entry.getKey().replaceFirst("^node-", "");
            CpuInfo cpu = entry.getValue();
            sb.append(String.format("| %s | %s | %s | %s |%n",
                    nodeShort,
                    cpu.model != null ? cpu.model : "-",
                    cpu.cores != null ? cpu.cores : "-",
                    cpu.arch != null ? cpu.arch : "-"));
        }
        return sb.toString();
    }

    /**
     * Build GPU summary table without reporting.
     * @return markdown table or null if no data
     */
    private String buildGpuSummary(long sessionId) throws SQLException {
        String sql = "SELECT node_id, message FROM logs " +
                     "WHERE session_id = ? AND message LIKE '%GPU INFO%' " +
                     "ORDER BY node_id, timestamp";

        Map<String, GpuFullInfo> nodeGpus = new LinkedHashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("node_id");
                    String message = rs.getString("message");
                    GpuFullInfo gpuInfo = nodeGpus.computeIfAbsent(nodeId, k -> new GpuFullInfo());

                    for (String line : message.split("\n")) {
                        String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();
                        if (cleanLine.contains("GPU INFO") || cleanLine.isEmpty()) continue;

                        // Parse CUDA_VERSION line
                        if (cleanLine.startsWith("CUDA_VERSION:")) {
                            gpuInfo.cudaVersion = cleanLine.replaceFirst("CUDA_VERSION:\\s*", "").trim();
                            continue;
                        }

                        // Parse nvidia-smi CSV output: name, memory.total, driver_version, compute_cap
                        // Example: "NVIDIA GeForce RTX 4080, 16384 MiB, 550.54.14, 8.9"
                        Pattern nvidiaCsvPattern = Pattern.compile(
                            "^(NVIDIA [^,]+|[^,]*GeForce[^,]*|[^,]*Quadro[^,]*|[^,]*Tesla[^,]*|[^,]*A100[^,]*|[^,]*H100[^,]*|[^,]*GB[0-9]+[^,]*),\\s*(\\d+)\\s*MiB,\\s*([\\d.]+),\\s*([\\d.]+)$"
                        );
                        Matcher nvidiaMatcher = nvidiaCsvPattern.matcher(cleanLine);
                        if (nvidiaMatcher.find()) {
                            gpuInfo.name = nvidiaMatcher.group(1).trim();
                            int vramMB = Integer.parseInt(nvidiaMatcher.group(2));
                            gpuInfo.vram = (vramMB >= 1024) ? (vramMB / 1024) + "GB" : vramMB + "MB";
                            gpuInfo.driver = nvidiaMatcher.group(3).trim();
                            gpuInfo.computeCap = nvidiaMatcher.group(4).trim();
                            continue;
                        }

                        // Parse lspci output for AMD/Intel GPUs
                        Pattern lspciPattern = Pattern.compile(
                            "(?:VGA compatible controller|3D controller|Display controller):\\s*(.+?)(?:\\s*\\(rev|$)");
                        Matcher lspciMatcher = lspciPattern.matcher(cleanLine);
                        if (lspciMatcher.find()) {
                            String gpuName = lspciMatcher.group(1).trim();
                            if (gpuInfo.name == null) {
                                gpuInfo.name = gpuName;
                                gpuInfo.driver = gpuName.contains("AMD") ? "amdgpu" : "-";
                                gpuInfo.vram = "-";
                                gpuInfo.computeCap = "-";
                                gpuInfo.cudaVersion = "-";
                            }
                        }
                    }
                }
            }
        }

        if (nodeGpus.isEmpty()) return null;

        // Count GPU types
        int nvidiaCount = 0, amdCount = 0, otherCount = 0;
        for (GpuFullInfo gpu : nodeGpus.values()) {
            if (gpu.name != null) {
                if (gpu.name.contains("NVIDIA") || gpu.name.contains("GeForce") ||
                    gpu.name.contains("Quadro") || gpu.name.contains("Tesla")) {
                    nvidiaCount++;
                } else if (gpu.name.contains("AMD") || gpu.name.contains("Radeon")) {
                    amdCount++;
                } else {
                    otherCount++;
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("## GPU Summary\n");
        sb.append("| node | gpu | vram | driver | cuda | compute_cap |\n");
        sb.append("|------|-----|------|--------|------|-------------|\n");
        for (Map.Entry<String, GpuFullInfo> entry : nodeGpus.entrySet()) {
            String nodeShort = entry.getKey().replaceFirst("^node-", "");
            GpuFullInfo gpu = entry.getValue();
            if (gpu.name != null) {
                sb.append(String.format("| %s | %s | %s | %s | %s | %s |%n",
                        nodeShort,
                        gpu.name,
                        gpu.vram != null ? gpu.vram : "-",
                        gpu.driver != null ? gpu.driver : "-",
                        gpu.cudaVersion != null ? gpu.cudaVersion : "-",
                        gpu.computeCap != null ? gpu.computeCap : "-"));
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
     * GPU full information holder.
     */
    private static class GpuFullInfo {
        String name;
        String vram;
        String driver;
        String cudaVersion;
        String computeCap;
    }

    /**
     * Build Memory summary table without reporting.
     * @return markdown table or null if no data
     */
    private String buildMemorySummary(long sessionId) throws SQLException {
        String sql = "SELECT node_id, message FROM logs " +
                     "WHERE session_id = ? AND message LIKE '%MEMORY INFO%' " +
                     "ORDER BY node_id, timestamp";

        Map<String, MemoryInfo> nodeMemory = new LinkedHashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("node_id");
                    String message = rs.getString("message");
                    MemoryInfo mem = nodeMemory.computeIfAbsent(nodeId, k -> new MemoryInfo());
                    for (String line : message.split("\n")) {
                        String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();
                        if (cleanLine.startsWith("Mem:")) {
                            Pattern memPattern = Pattern.compile(
                                "Mem:\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?(?:\\s+(\\S+))?(?:\\s+(\\S+))?");
                            Matcher m = memPattern.matcher(cleanLine);
                            if (m.find()) {
                                mem.total = m.group(1);
                                mem.used = m.group(2);
                                mem.free = m.group(3);
                                if (m.group(6) != null) mem.available = m.group(6);
                            }
                        } else if (cleanLine.startsWith("Swap:")) {
                            Pattern swapPattern = Pattern.compile("Swap:\\s+(\\S+)");
                            Matcher m = swapPattern.matcher(cleanLine);
                            if (m.find()) mem.swap = m.group(1);
                        }
                    }
                }
            }
        }

        if (nodeMemory.isEmpty()) return null;

        StringBuilder sb = new StringBuilder();
        sb.append("## Memory Summary\n");
        sb.append("| node | total | used | free | available | swap |\n");
        sb.append("|------|-------|------|------|-----------|------|\n");
        for (Map.Entry<String, MemoryInfo> entry : nodeMemory.entrySet()) {
            String nodeShort = entry.getKey().replaceFirst("^node-", "");
            MemoryInfo mem = entry.getValue();
            sb.append(String.format("| %s | %s | %s | %s | %s | %s |%n",
                    nodeShort,
                    mem.total != null ? mem.total : "-",
                    mem.used != null ? mem.used : "-",
                    mem.free != null ? mem.free : "-",
                    mem.available != null ? mem.available : "-",
                    mem.swap != null ? mem.swap : "-"));
        }
        return sb.toString();
    }

    /**
     * Build Disk summary table without reporting.
     * @return markdown table or null if no data
     */
    private String buildDiskSummary(long sessionId) throws SQLException {
        String sql = "SELECT node_id, message FROM logs " +
                     "WHERE session_id = ? AND message LIKE '%DISK INFO%' " +
                     "ORDER BY node_id, timestamp";

        Map<String, List<DiskInfo>> nodeDisks = new LinkedHashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("node_id");
                    String message = rs.getString("message");
                    for (String line : message.split("\n")) {
                        String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();
                        Pattern diskPattern = Pattern.compile(
                            "(sd[a-z]+|nvme\\d+n\\d+)\\s+([\\d.]+[GMTP])\\s+disk\\s+(.+)$");
                        Matcher m = diskPattern.matcher(cleanLine);
                        if (m.find()) {
                            nodeDisks.computeIfAbsent(nodeId, k -> new ArrayList<>())
                                .add(new DiskInfo(m.group(1), m.group(2), m.group(3).trim()));
                        }
                    }
                }
            }
        }

        if (nodeDisks.isEmpty()) return null;

        StringBuilder sb = new StringBuilder();
        sb.append("| node | disk | model | size |\n");
        sb.append("|------|------|-------|------|\n");
        for (Map.Entry<String, List<DiskInfo>> entry : nodeDisks.entrySet()) {
            String nodeShort = entry.getKey().replaceFirst("^node-", "");
            for (DiskInfo disk : entry.getValue()) {
                sb.append(String.format("| %s | %s | %s | %s |%n",
                        nodeShort, disk.device, disk.model, disk.size));
            }
        }
        return sb.toString();
    }

    /**
     * Build Network summary table without reporting.
     * @return markdown table or null if no data
     */
    private String buildNetworkSummary(long sessionId) throws SQLException {
        String sql = "SELECT node_id, message FROM logs " +
                     "WHERE session_id = ? AND message LIKE '%NETWORK INFO%' " +
                     "ORDER BY node_id, timestamp";

        Map<String, List<NetworkInfo>> nodeNetworks = new LinkedHashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("node_id");
                    String message = rs.getString("message");
                    List<NetworkInfo> nets = nodeNetworks.computeIfAbsent(nodeId, k -> new ArrayList<>());
                    NetworkInfo currentNet = null;
                    for (String line : message.split("\n")) {
                        String cleanLine = line.replaceFirst("^\\[node-[^\\]]+\\]\\s*", "").trim();
                        Pattern ifPattern = Pattern.compile("^\\d+:\\s+(\\S+):\\s+<.*>.*state\\s+(\\S+)");
                        Matcher ifMatcher = ifPattern.matcher(cleanLine);
                        if (ifMatcher.find()) {
                            currentNet = new NetworkInfo();
                            currentNet.iface = ifMatcher.group(1);
                            currentNet.state = ifMatcher.group(2);
                            nets.add(currentNet);
                            continue;
                        }
                        Pattern inetPattern = Pattern.compile("inet\\s+([\\d.]+(?:/\\d+)?)");
                        Matcher inetMatcher = inetPattern.matcher(cleanLine);
                        if (inetMatcher.find() && currentNet != null && currentNet.ip == null) {
                            currentNet.ip = inetMatcher.group(1);
                        }
                        Pattern macPattern = Pattern.compile("ether\\s+([0-9a-f:]+)");
                        Matcher macMatcher = macPattern.matcher(cleanLine);
                        if (macMatcher.find() && currentNet != null && currentNet.mac == null) {
                            currentNet.mac = macMatcher.group(1);
                        }
                    }
                }
            }
        }

        if (nodeNetworks.isEmpty()) return null;

        StringBuilder sb = new StringBuilder();
        sb.append("## Network Summary\n");
        sb.append("| node | interface | state | ip | mac |\n");
        sb.append("|------|-----------|-------|-----|-----|\n");
        for (Map.Entry<String, List<NetworkInfo>> entry : nodeNetworks.entrySet()) {
            String nodeShort = entry.getKey().replaceFirst("^node-", "");
            for (NetworkInfo net : entry.getValue()) {
                if (net.iface != null && net.iface.equals("lo")) continue;
                sb.append(String.format("| %s | %s | %s | %s | %s |%n",
                        nodeShort,
                        net.iface != null ? net.iface : "-",
                        net.state != null ? net.state : "-",
                        net.ip != null ? net.ip : "-",
                        net.mac != null ? net.mac : "-"));
            }
        }
        return sb.toString();
    }

    // ========== End of internal build methods ==========

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
     * Disk information holder.
     */
    private record DiskInfo(String device, String size, String model) {}

    /**
     * CPU information holder.
     */
    private static class CpuInfo {
        String model;
        String cores;
        String arch;
    }

    /**
     * Memory information holder.
     */
    private static class MemoryInfo {
        String total;
        String used;
        String free;
        String available;
        String swap;
    }

    /**
     * Network information holder.
     */
    private static class NetworkInfo {
        String iface;
        String state;
        String ip;
        String mac;
    }
}
