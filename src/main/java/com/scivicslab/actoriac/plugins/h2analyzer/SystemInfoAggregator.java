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

import java.sql.*;
import java.util.*;
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
 *   <li>list-sessions - List available sessions</li>
 *   <li>node-status - Show node status summary. Args: sessionId</li>
 * </ul>
 *
 * @author devteam@scivics-lab.com
 * @since 1.0.0
 */
public class SystemInfoAggregator implements CallableByActionName, ActorSystemAware {

    private Connection connection;
    private String currentDbPath;
    private IIActorSystem system;

    @Override
    public void setActorSystem(IIActorSystem system) {
        this.system = system;
    }

    @Override
    public ActionResult callByActionName(String actionName, String args) {
        try {
            return switch (actionName) {
                case "connect" -> connect(args);
                case "summarize-disks" -> summarizeDisks(args);
                case "list-sessions" -> listSessions();
                case "node-status" -> nodeStatus(args);
                case "disconnect" -> disconnect();
                default -> new ActionResult(false, "Unknown action: " + actionName);
            };
        } catch (Exception e) {
            return new ActionResult(false, "Error: " + e.getMessage());
        }
    }

    /**
     * Connect to H2 database.
     *
     * @param dbPath path to database (without .mv.db extension)
     */
    private ActionResult connect(String dbPath) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }

            // Use H2 Driver directly to avoid DriverManager classloader issues
            // when this plugin is loaded via URLClassLoader
            org.h2.Driver driver = new org.h2.Driver();
            String url = "jdbc:h2:" + dbPath.trim() + ";AUTO_SERVER=TRUE";
            connection = driver.connect(url, new java.util.Properties());
            currentDbPath = dbPath.trim();

            if (connection == null) {
                return new ActionResult(false, "Connection returned null for: " + dbPath);
            }

            return new ActionResult(true, "Connected to: " + dbPath);
        } catch (SQLException e) {
            return new ActionResult(false, "Connection failed: " + e.getMessage());
        }
    }

    /**
     * Disconnect from database.
     */
    private ActionResult disconnect() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                connection = null;
            }
            return new ActionResult(true, "Disconnected");
        } catch (SQLException e) {
            return new ActionResult(false, "Disconnect failed: " + e.getMessage());
        }
    }

    /**
     * List available sessions.
     */
    private ActionResult listSessions() {
        if (connection == null) {
            return new ActionResult(false, "Not connected. Use 'connect' first.");
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

            return new ActionResult(true, sb.toString());
        } catch (SQLException e) {
            return new ActionResult(false, "Query failed: " + e.getMessage());
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
        if (connection == null) {
            return new ActionResult(false, "Not connected. Use 'connect' first.");
        }

        try {
            long sessionId;

            // Auto-retrieve session ID from nodeGroup if not specified
            if (sessionIdStr == null || sessionIdStr.trim().isEmpty() || sessionIdStr.equals("[]")) {
                String autoSessionId = getSessionIdFromNodeGroup();
                if (autoSessionId == null) {
                    return new ActionResult(false,
                        "Session ID not specified and could not retrieve from nodeGroup");
                }
                sessionId = Long.parseLong(autoSessionId);
            } else {
                sessionId = Long.parseLong(sessionIdStr.trim());
            }

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
                return new ActionResult(true, "No disk information found in session " + sessionId);
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

            return new ActionResult(true, sb.toString());

        } catch (NumberFormatException e) {
            return new ActionResult(false, "Invalid session ID: " + sessionIdStr);
        } catch (SQLException e) {
            return new ActionResult(false, "Query failed: " + e.getMessage());
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
        if (connection == null) {
            return new ActionResult(false, "Not connected. Use 'connect' first.");
        }

        try {
            long sessionId;

            // Auto-retrieve session ID from nodeGroup if not specified
            if (sessionIdStr == null || sessionIdStr.trim().isEmpty() || sessionIdStr.equals("[]")) {
                String autoSessionId = getSessionIdFromNodeGroup();
                if (autoSessionId == null) {
                    return new ActionResult(false,
                        "Session ID not specified and could not retrieve from nodeGroup");
                }
                sessionId = Long.parseLong(autoSessionId);
            } else {
                sessionId = Long.parseLong(sessionIdStr.trim());
            }

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

            return new ActionResult(true, sb.toString());

        } catch (NumberFormatException e) {
            return new ActionResult(false, "Invalid session ID: " + sessionIdStr);
        } catch (SQLException e) {
            return new ActionResult(false, "Query failed: " + e.getMessage());
        }
    }

    /**
     * Retrieves session ID from nodeGroup actor.
     *
     * @return session ID string, or null if not available
     */
    private String getSessionIdFromNodeGroup() {
        if (system == null) {
            return null;
        }
        IIActorRef<?> nodeGroup = system.getIIActor("nodeGroup");
        if (nodeGroup == null) {
            return null;
        }
        ActionResult result = nodeGroup.callByActionName("getSessionId", "");
        return result.isSuccess() ? result.getResult() : null;
    }

    /**
     * Disk information holder.
     */
    private record DiskInfo(String device, String size, String model) {}
}
