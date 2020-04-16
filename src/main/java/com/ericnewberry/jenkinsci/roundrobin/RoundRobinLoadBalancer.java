/*
 * The MIT License
 *
 * Copyright (c) 2020, Eric Newberry <eric@ericnewberry.com>
 * Modified from LeastLoadPlugin, Copyright (c) 2013, Brendan Nolan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.ericnewberry.jenkinsci.roundrobin;

import com.google.common.base.Preconditions;
import hudson.model.Executor;
import hudson.model.Job;
import hudson.model.LoadBalancer;
import hudson.model.Queue.Task;
import hudson.model.queue.MappingWorksheet;
import hudson.model.queue.MappingWorksheet.ExecutorChunk;
import hudson.model.queue.MappingWorksheet.Mapping;
import hudson.model.queue.MappingWorksheet.WorkChunk;
import hudson.model.queue.SubTask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Logger;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.WARNING;

public class RoundRobinLoadBalancer extends LoadBalancer {
    private static final Logger LOGGER = Logger.getLogger(RoundRobinLoadBalancer.class.getCanonicalName());
    private static final Comparator<ExecutorChunk> EXECUTOR_CHUNK_COMPARATOR = new ExecutorChunkComparator();
    private final LoadBalancer fallback;
    private HashMap<String, String> lastRunner;

    public RoundRobinLoadBalancer(LoadBalancer fallback) {
        Preconditions.checkNotNull(fallback, "You must provide a fallback implementation of the LoadBalancer");
        this.fallback = fallback;
        this.lastRunner = new LinkedHashMap<String, String>();
    }

    @Override
    public Mapping map(Task task, MappingWorksheet ws) {
        try {
            if (!isDisabled(task)) {
                Mapping m = ws.new Mapping();
                if (!assignRoundRobin(m)) {
                    LOGGER.log(WARNING, "Round robin load balancer failed - will use fallback");
                    return getFallBackLoadBalancer().map(task, ws);
                }
                else {
                    return m;
                }
            }
            else {
                return getFallBackLoadBalancer().map(task, ws);
            }
        }
        catch (Exception e) {
            LOGGER.log(WARNING, "Round robin load balancer failed - will use fallback", e);
            return getFallBackLoadBalancer().map(task, ws);
        }
    }

    private List<ExecutorChunk> getApplicableSorted(WorkChunk c) {
        List<ExecutorChunk> valid = c.applicableExecutorChunks();
        Collections.sort(valid, EXECUTOR_CHUNK_COMPARATOR);
        return valid;
    }

    @SuppressWarnings("rawtypes")
    private boolean isDisabled(Task task) {
        SubTask subTask = task.getOwnerTask();

        if (subTask instanceof Job) {
            Job job = (Job) subTask;
            @SuppressWarnings("unchecked")
            RoundRobinDisabledProperty property = (RoundRobinDisabledProperty) job.getProperty(RoundRobinDisabledProperty.class);
            if (property != null) {
                return property.isRoundRobinDisabled();
            }
            return false;
        }
        else {
            return true;
        }
    }

    private boolean assignRoundRobin(Mapping mapping) {
        for (int workchunk = 0; workchunk < mapping.size(); workchunk++) {
            String label = mapping.get(workchunk).get(0).getAssignedLabel().getExpression();
            String assignedExecutor = "";

            // Get applicable executors, sorted by name
            List<ExecutorChunk> executors = getApplicableSorted(mapping.get(workchunk));

            if (executors.size() == 0) {
                // No available executors - fall back to default load balancer
                LOGGER.log(WARNING, "Unable to schedule '" + label + "': No available executors");
                return false;
            }

            // Check whether work chunk has never been scheduled before or last runner is no more
            boolean isLastRunnerValid = false;

            if (lastRunner.containsKey(label)) {
                for (ExecutorChunk executor : executors) {
                    if (executor.getName().compareTo(lastRunner.get(label)) == 0) {
                        isLastRunnerValid = true;
                    }
                }
            }

            if (!isLastRunnerValid) {
                // This label has not been scheduled yet or previous executor no longer exists
                // Therefore, run on first node alphabetically
                LOGGER.log(FINE, "First time scheduling label '" + label + "' or previous last runner gone");
                lastRunner.put(label, executors.get(0).getName());
                assignedExecutor = executors.get(0).getName();
                mapping.assign(workchunk, executors.get(0));
            }
            else {
                // Schedule using round robin from last executor
                int lastPos = -1;
                for (int i = 0; i < executors.size(); i++) {
                    if (executors.get(i).getName().compareTo(lastRunner.get(label)) == 0) {
                        lastPos = i;
                        break;
                    }
                }

                if (lastPos == -1) {
                    // Internal error
                    LOGGER.log(WARNING, "Unable to schedule '" + label + "': Couldn't find last executor position");
                    return false;
                }

                // Schedule on next node
                int runner = (lastPos + 1) % executors.size();
                lastRunner.put(label, executors.get(runner).getName());
                assignedExecutor = executors.get(runner).getName();
                mapping.assign(workchunk, executors.get(runner));
            }

            if (!mapping.isPartiallyValid()) {
                LOGGER.log(WARNING, "Unable to schedule '" + label + "': Mapping to '" + assignedExecutor + "' not partially valid");
                return false;
            }

            LOGGER.log(FINE, "Scheduling label '" + label + "' on '" + assignedExecutor + "'");
        }

        return true;
    }

    public LoadBalancer getFallBackLoadBalancer() {
        return fallback;
    }

    protected static class ExecutorChunkComparator implements Comparator<ExecutorChunk>, Serializable {
        public int compare(ExecutorChunk ec1, ExecutorChunk ec2) {
            return ec1.getName().compareTo(ec2.getName());
        }
    }
}
