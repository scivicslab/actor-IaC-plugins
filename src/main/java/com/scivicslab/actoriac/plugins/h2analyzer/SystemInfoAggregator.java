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
     * <p>Connection priority:</p>
     * <ol>
     *   <li>Try TCP connection to log server (localhost:29090) first</li>
     *   <li>Fall back to embedded mode with AUTO_SERVER=TRUE if TCP fails</li>
     * </ol>
     *
     * <p>This design ensures the plugin connects to the same database that
     * actor-IaC is using for logging, even when actor-IaC has started a
     * background TCP server.</p>
     *
     * @param dbPath path to database (without .mv.db extension)
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

            // Use H2 Driver directly to avoid DriverManager classloader issues
            // when this plugin is loaded via URLClassLoader
            org.h2.Driver driver = new org.h2.Driver();
            currentDbPath = dbPath;

            // Convert relative path to canonical (normalized) absolute path for TCP URL
            // Important: Use getCanonicalPath() to normalize "./path" to "/absolute/path"
            // so H2 TCP server recognizes it as the same database
            java.io.File dbFile = new java.io.File(dbPath);
            String absolutePath = dbFile.getCanonicalPath();

            // Try TCP connection first (to existing log server on port 29090)
            String tcpUrl = "jdbc:h2:tcp://localhost:29090/" + absolutePath;
            try {
                connection = driver.connect(tcpUrl, new java.util.Properties());
                if (connection != null) {
                    logger.logp(Level.INFO, CLASS_NAME, "connect",
                        "Connected via TCP to: {0}", tcpUrl);
                    ActionResult result = new ActionResult(true,
                        "Connected via TCP to: " + dbPath);
                    logger.exiting(CLASS_NAME, "connect", result);
                    return result;
                }
            } catch (SQLException tcpEx) {
                logger.logp(Level.FINE, CLASS_NAME, "connect",
                    "TCP connection failed, trying embedded mode: {0}", tcpEx.getMessage());
            }

            // Fall back to embedded mode with AUTO_SERVER
            String embeddedUrl = "jdbc:h2:" + dbPath + ";AUTO_SERVER=TRUE";
            connection = driver.connect(embeddedUrl, new java.util.Properties());

            if (connection == null) {
                ActionResult result = new ActionResult(false, "Connection returned null for: " + dbPath);
                logger.exiting(CLASS_NAME, "connect", result);
                return result;
            }

            logger.logp(Level.INFO, CLASS_NAME, "connect",
                "Connected in embedded mode to: {0}", embeddedUrl);
            ActionResult result = new ActionResult(true, "Connected (embedded) to: " + dbPath);
            logger.exiting(CLASS_NAME, "connect", result);
            return result;
        } catch (SQLException e) {
            ActionResult result = new ActionResult(false, "Connection failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "connect", "SQLException occurred", e);
            logger.exiting(CLASS_NAME, "connect", result);
            return result;
        } catch (java.io.IOException e) {
            ActionResult result = new ActionResult(false, "Path resolution failed: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "connect", "IOException occurred", e);
            logger.exiting(CLASS_NAME, "connect", result);
            return result;
        }
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
                         "WHERE session_id = ? AND message LIKE '%disk%' " +
                         "ORDER BY node_id, timestamp";

            Map<String, List<DiskInfo>> nodeDisks = new LinkedHashMap<>();

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id");
                        String message = rs.getString("message");

                        // Parse disk info from message
                        // Pattern: sda 1.9T disk TS2TSSD230S
                        //          nvme0n1 931.5G disk CSSD-M2B1TPG3NF2
                        Pattern diskPattern = Pattern.compile(
                            "(sd[a-z]|nvme\\d+n\\d+)\\s+([\\d.]+[GMTP])\\s+disk\\s+(.+)$"
                        );
                        Matcher m = diskPattern.matcher(message);
                        if (m.find()) {
                            DiskInfo disk = new DiskInfo(m.group(1), m.group(2), m.group(3).trim());
                            nodeDisks.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(disk);
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
        arg.put("type", "plugin-result");
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

            // Extract CPU-related log entries (lscpu output)
            String sql = "SELECT node_id, message FROM logs " +
                         "WHERE session_id = ? AND (message LIKE '%Model name%' OR message LIKE '%CPU(s):%' " +
                         "OR message LIKE '%Architecture%' OR message LIKE '%Thread(s) per core%') " +
                         "ORDER BY node_id, timestamp";

            Map<String, CpuInfo> nodeCpus = new LinkedHashMap<>();

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id");
                        String message = rs.getString("message");

                        CpuInfo cpu = nodeCpus.computeIfAbsent(nodeId, k -> new CpuInfo());

                        // Parse: Model name: Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz
                        if (message.contains("Model name:")) {
                            cpu.model = message.replaceFirst(".*Model name:\\s*", "").trim();
                        }
                        // Parse: CPU(s): 16
                        else if (message.matches(".*CPU\\(s\\):\\s*\\d+.*")) {
                            Matcher m = Pattern.compile("CPU\\(s\\):\\s*(\\d+)").matcher(message);
                            if (m.find()) {
                                cpu.cores = m.group(1);
                            }
                        }
                        // Parse: Architecture: x86_64
                        else if (message.contains("Architecture:")) {
                            cpu.arch = message.replaceFirst(".*Architecture:\\s*", "").trim();
                        }
                    }
                }
            }

            if (nodeCpus.isEmpty()) {
                String resultStr = "No CPU information found in session " + sessionId;
                reportToMultiplexer(resultStr);
                ActionResult result = new ActionResult(true, resultStr);
                logger.exiting(CLASS_NAME, "summarizeCpu", result);
                return result;
            }

            // Build markdown table
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

            String resultStr = sb.toString();
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

            // Extract GPU-related log entries (lspci VGA, nvidia-smi)
            String sql = "SELECT node_id, message FROM logs " +
                         "WHERE session_id = ? AND (message LIKE '%VGA%' OR message LIKE '%3D controller%' " +
                         "OR message LIKE '%NVIDIA%' OR message LIKE '%GeForce%' OR message LIKE '%Radeon%') " +
                         "ORDER BY node_id, timestamp";

            Map<String, List<String>> nodeGpus = new LinkedHashMap<>();

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id");
                        String message = rs.getString("message");

                        // Parse GPU name from lspci output
                        // Pattern: VGA compatible controller: NVIDIA Corporation ... [GeForce RTX 3080]
                        //          3D controller: NVIDIA Corporation ...
                        Pattern gpuPattern = Pattern.compile(
                            "(?:VGA compatible controller|3D controller):\\s*(.+?)(?:\\s*\\(rev|$)"
                        );
                        Matcher m = gpuPattern.matcher(message);
                        if (m.find()) {
                            String gpu = m.group(1).trim();
                            nodeGpus.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(gpu);
                        }
                    }
                }
            }

            if (nodeGpus.isEmpty()) {
                String resultStr = "No GPU information found in session " + sessionId;
                reportToMultiplexer(resultStr);
                ActionResult result = new ActionResult(true, resultStr);
                logger.exiting(CLASS_NAME, "summarizeGpu", result);
                return result;
            }

            // Build markdown table
            StringBuilder sb = new StringBuilder();
            sb.append("## GPU Summary\n");
            sb.append("| node | gpu |\n");
            sb.append("|------|-----|\n");

            for (Map.Entry<String, List<String>> entry : nodeGpus.entrySet()) {
                String nodeShort = entry.getKey().replaceFirst("^node-", "");
                for (String gpu : entry.getValue()) {
                    sb.append(String.format("| %s | %s |%n", nodeShort, gpu));
                }
            }

            String resultStr = sb.toString();
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

            // Extract memory-related log entries (free -h, /proc/meminfo)
            String sql = "SELECT node_id, message FROM logs " +
                         "WHERE session_id = ? AND (message LIKE '%Mem:%' OR message LIKE '%MemTotal%' " +
                         "OR message LIKE '%MemAvailable%' OR message LIKE '%Swap:%') " +
                         "ORDER BY node_id, timestamp";

            Map<String, MemoryInfo> nodeMemory = new LinkedHashMap<>();

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id");
                        String message = rs.getString("message");

                        MemoryInfo mem = nodeMemory.computeIfAbsent(nodeId, k -> new MemoryInfo());

                        // Parse: Mem:           31Gi       8.2Gi        20Gi
                        // Format: Mem: total used free shared buff/cache available
                        if (message.contains("Mem:") && !message.contains("MemTotal")) {
                            Pattern memPattern = Pattern.compile(
                                "Mem:\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)(?:\\s+(\\S+))?(?:\\s+(\\S+))?(?:\\s+(\\S+))?"
                            );
                            Matcher m = memPattern.matcher(message);
                            if (m.find()) {
                                mem.total = m.group(1);
                                mem.used = m.group(2);
                                mem.free = m.group(3);
                                if (m.group(6) != null) {
                                    mem.available = m.group(6);
                                }
                            }
                        }
                        // Parse: Swap:         8.0Gi          0B       8.0Gi
                        else if (message.contains("Swap:")) {
                            Pattern swapPattern = Pattern.compile("Swap:\\s+(\\S+)");
                            Matcher m = swapPattern.matcher(message);
                            if (m.find()) {
                                mem.swap = m.group(1);
                            }
                        }
                    }
                }
            }

            if (nodeMemory.isEmpty()) {
                String resultStr = "No memory information found in session " + sessionId;
                reportToMultiplexer(resultStr);
                ActionResult result = new ActionResult(true, resultStr);
                logger.exiting(CLASS_NAME, "summarizeMemory", result);
                return result;
            }

            // Build markdown table
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

            String resultStr = sb.toString();
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

            // Extract network-related log entries (ip addr output)
            String sql = "SELECT node_id, message FROM logs " +
                         "WHERE session_id = ? AND (message LIKE '%inet %' OR message LIKE '%ether %' " +
                         "OR message LIKE '%: <%' OR message LIKE '%state UP%' OR message LIKE '%state DOWN%') " +
                         "ORDER BY node_id, timestamp";

            Map<String, List<NetworkInfo>> nodeNetworks = new LinkedHashMap<>();
            Map<String, String> currentInterface = new HashMap<>();

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String nodeId = rs.getString("node_id");
                        String message = rs.getString("message");

                        // Parse interface line: 2: enp0s31f6: <BROADCAST,...> ... state UP
                        Pattern ifPattern = Pattern.compile("^\\d+:\\s+(\\S+):\\s+<.*>.*state\\s+(\\S+)");
                        Matcher ifMatcher = ifPattern.matcher(message);
                        if (ifMatcher.find()) {
                            String ifName = ifMatcher.group(1);
                            String state = ifMatcher.group(2);
                            currentInterface.put(nodeId, ifName);
                            NetworkInfo net = new NetworkInfo();
                            net.iface = ifName;
                            net.state = state;
                            nodeNetworks.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(net);
                            continue;
                        }

                        // Parse inet line: inet 192.168.5.13/24 brd 192.168.5.255
                        Pattern inetPattern = Pattern.compile("inet\\s+([\\d.]+/\\d+)");
                        Matcher inetMatcher = inetPattern.matcher(message);
                        if (inetMatcher.find()) {
                            String ip = inetMatcher.group(1);
                            List<NetworkInfo> nets = nodeNetworks.get(nodeId);
                            if (nets != null && !nets.isEmpty()) {
                                NetworkInfo lastNet = nets.get(nets.size() - 1);
                                if (lastNet.ip == null) {
                                    lastNet.ip = ip;
                                }
                            }
                        }

                        // Parse MAC address: link/ether aa:bb:cc:dd:ee:ff
                        Pattern macPattern = Pattern.compile("ether\\s+([0-9a-f:]+)");
                        Matcher macMatcher = macPattern.matcher(message);
                        if (macMatcher.find()) {
                            String mac = macMatcher.group(1);
                            List<NetworkInfo> nets = nodeNetworks.get(nodeId);
                            if (nets != null && !nets.isEmpty()) {
                                NetworkInfo lastNet = nets.get(nets.size() - 1);
                                if (lastNet.mac == null) {
                                    lastNet.mac = mac;
                                }
                            }
                        }
                    }
                }
            }

            if (nodeNetworks.isEmpty()) {
                String resultStr = "No network information found in session " + sessionId;
                reportToMultiplexer(resultStr);
                ActionResult result = new ActionResult(true, resultStr);
                logger.exiting(CLASS_NAME, "summarizeNetwork", result);
                return result;
            }

            // Build markdown table
            StringBuilder sb = new StringBuilder();
            sb.append("## Network Summary\n");
            sb.append("| node | interface | state | ip | mac |\n");
            sb.append("|------|-----------|-------|-----|-----|\n");

            for (Map.Entry<String, List<NetworkInfo>> entry : nodeNetworks.entrySet()) {
                String nodeShort = entry.getKey().replaceFirst("^node-", "");
                for (NetworkInfo net : entry.getValue()) {
                    // Skip loopback
                    if (net.iface != null && net.iface.equals("lo")) {
                        continue;
                    }
                    sb.append(String.format("| %s | %s | %s | %s | %s |%n",
                            nodeShort,
                            net.iface != null ? net.iface : "-",
                            net.state != null ? net.state : "-",
                            net.ip != null ? net.ip : "-",
                            net.mac != null ? net.mac : "-"));
                }
            }

            String resultStr = sb.toString();
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
            String sessionIdResolved = String.valueOf(sessionId);

            StringBuilder sb = new StringBuilder();
            sb.append("# System Information Summary (Session #").append(sessionId).append(")\n\n");

            // Collect all summaries
            ActionResult cpuResult = summarizeCpu(sessionIdResolved);
            if (cpuResult.isSuccess() && !cpuResult.getResult().contains("No CPU information")) {
                sb.append(cpuResult.getResult()).append("\n");
            }

            ActionResult gpuResult = summarizeGpu(sessionIdResolved);
            if (gpuResult.isSuccess() && !gpuResult.getResult().contains("No GPU information")) {
                sb.append(gpuResult.getResult()).append("\n");
            }

            ActionResult memResult = summarizeMemory(sessionIdResolved);
            if (memResult.isSuccess() && !memResult.getResult().contains("No memory information")) {
                sb.append(memResult.getResult()).append("\n");
            }

            ActionResult diskResult = summarizeDisks(sessionIdResolved);
            if (diskResult.isSuccess() && !diskResult.getResult().contains("No disk information")) {
                sb.append("## Disk Summary\n");
                sb.append(diskResult.getResult()).append("\n");
            }

            ActionResult netResult = summarizeNetwork(sessionIdResolved);
            if (netResult.isSuccess() && !netResult.getResult().contains("No network information")) {
                sb.append(netResult.getResult()).append("\n");
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
