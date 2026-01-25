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

package com.scivicslab.actoriac.plugins.aptcheck;

import com.scivicslab.pojoactor.core.ActionResult;
import com.scivicslab.pojoactor.core.CallableByActionName;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Apt Lock Checker for actor-IaC workflows.
 *
 * <p>Parses `ps -eo pid,etimes,args` output to detect hung apt processes.</p>
 *
 * <h2>Usage in workflow:</h2>
 * <pre>
 * - states: ["0", "1"]
 *   actions:
 *     - actor: loader
 *       method: createChild
 *       arguments: ["this", "aptChecker", "com.scivicslab.actoriac.plugins.aptcheck.AptLockChecker"]
 *
 * - states: ["1", "2"]
 *   actions:
 *     - actor: this
 *       method: executeCommand
 *       arguments: ["hostname -s"]
 *
 * - states: ["2", "3"]
 *   actions:
 *     - actor: this
 *       method: putJson
 *       arguments:
 *         path: hostname
 *         value: "${result}"
 *
 * - states: ["3", "4"]
 *   actions:
 *     - actor: this
 *       method: executeCommand
 *       arguments: ["ps -eo pid,etimes,args 2>/dev/null"]
 *
 * - states: ["4", "5"]
 *   actions:
 *     - actor: aptChecker
 *       method: check
 *       arguments:
 *         psOutput: "${result}"
 *         hostname: "${json.hostname}"
 * </pre>
 *
 * @author devteam@scivics-lab.com
 * @since 1.0.0
 */
public class AptLockChecker implements CallableByActionName {

    private static final String CLASS_NAME = AptLockChecker.class.getName();
    private static final Logger logger = Logger.getLogger(CLASS_NAME);

    private static final long DEFAULT_THRESHOLD_SECONDS = 3600;

    private static final String[] APT_PROCESS_PATTERNS = {
        "/usr/lib/apt/apt.systemd.daily",
        "/usr/bin/unattended-upgrade"
    };

    @Override
    public ActionResult callByActionName(String actionName, String args) {
        logger.entering(CLASS_NAME, "callByActionName", new Object[]{actionName, args});
        try {
            ActionResult result = switch (actionName) {
                case "check" -> check(args);
                default -> new ActionResult(false, "Unknown action: " + actionName);
            };
            logger.exiting(CLASS_NAME, "callByActionName", result);
            return result;
        } catch (Exception e) {
            ActionResult errorResult = new ActionResult(false, "Error: " + e.getMessage());
            logger.logp(Level.WARNING, CLASS_NAME, "callByActionName", "Exception occurred", e);
            return errorResult;
        }
    }

    /**
     * Check ps output for hung apt processes.
     *
     * @param args JSON: {"psOutput": "...", "hostname": "...", "threshold": 3600}
     * @return "OK" if no hung processes, "HUNG:pid:age" if found
     */
    private ActionResult check(String args) {
        logger.entering(CLASS_NAME, "check", args);

        if (args == null || args.trim().isEmpty()) {
            return new ActionResult(false, "No arguments provided");
        }

        String psOutput;
        String hostname = "unknown";
        long threshold = DEFAULT_THRESHOLD_SECONDS;

        try {
            JSONObject json = new JSONObject(args);
            psOutput = json.getString("psOutput");
            hostname = json.optString("hostname", "unknown").trim();
            threshold = json.optLong("threshold", DEFAULT_THRESHOLD_SECONDS);
        } catch (Exception e) {
            return new ActionResult(false, "Invalid JSON: " + e.getMessage());
        }

        List<HungProcess> hungProcesses = findHungProcesses(psOutput, threshold);

        if (hungProcesses.isEmpty()) {
            return new ActionResult(true, "OK");
        } else {
            HungProcess first = hungProcesses.get(0);
            return new ActionResult(true, "HUNG:" + first.pid + ":" + first.ageHuman);
        }
    }

    private List<HungProcess> findHungProcesses(String psOutput, long threshold) {
        List<HungProcess> result = new ArrayList<>();
        Pattern linePattern = Pattern.compile("^\\s*(\\d+)\\s+(\\d+)\\s+(.+)$", Pattern.MULTILINE);

        Matcher matcher = linePattern.matcher(psOutput);
        while (matcher.find()) {
            try {
                int pid = Integer.parseInt(matcher.group(1));
                long etimeSeconds = Long.parseLong(matcher.group(2));
                String command = matcher.group(3).trim();

                boolean isAptProcess = false;
                for (String pattern : APT_PROCESS_PATTERNS) {
                    if (command.contains(pattern)) {
                        isAptProcess = true;
                        break;
                    }
                }

                if (isAptProcess && etimeSeconds >= threshold) {
                    HungProcess hp = new HungProcess();
                    hp.pid = pid;
                    hp.etimeSeconds = etimeSeconds;
                    hp.command = command;
                    hp.ageHuman = formatAge(etimeSeconds);
                    result.add(hp);
                }
            } catch (NumberFormatException e) {
                // Skip malformed lines
            }
        }
        return result;
    }

    private String formatAge(long seconds) {
        if (seconds >= 86400) return (seconds / 86400) + "d";
        if (seconds >= 3600) return (seconds / 3600) + "h";
        if (seconds >= 60) return (seconds / 60) + "m";
        return seconds + "s";
    }

    private static class HungProcess {
        int pid;
        long etimeSeconds;
        String ageHuman;
        String command;
    }
}
