package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobResourcesRequirementsUpdateHeaders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Single-shot rescale tool via Flink REST API.
 *
 * <p>Rescale logic:
 *
 * <ul>
 *   <li>If all vertices have the same parallelism, alternate +1 / -1 across vertices using a
 *       round-robin index (passed as argument to support successive invocations).
 *   <li>If vertices have different parallelisms, increase every vertex's parallelism by 1.
 * </ul>
 *
 * <p>Usage: FlinkRestClientDemo [restAddress] [jobId] [roundRobinIndex]
 */
public class FlinkRestClientDemo {

    private static final long REST_TIMEOUT_SECONDS = 30;

    public static void main(String[] args) throws Exception {
        String restAddress = args.length > 0 ? args[0] : "http://localhost:12345";
        String jobIdStr = args.length > 1 ? args[1] : null;
        int roundRobinIndex = args.length > 2 ? Integer.parseInt(args[2]) : 0;

        try (var client =
                new RestClusterClient<>(
                        new Configuration(),
                        "clusterId",
                        (c, e) -> new StandaloneClientHAServices(restAddress))) {

            JobID jobId;
            if (jobIdStr != null) {
                jobId = JobID.fromHexString(jobIdStr);
            } else {
                var jobs = client.listJobs().get(REST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                jobId =
                        jobs.stream()
                                .filter(j -> j.getJobState() == JobStatus.RUNNING)
                                .findFirst()
                                .orElseThrow(() -> new RuntimeException("No running job found"))
                                .getJobId();
                System.out.println("Auto-detected running job: " + jobId);
            }

            rescale(client, jobId, roundRobinIndex);
        }
    }

    private static void rescale(RestClusterClient<String> client, JobID jobId, int roundRobinIndex)
            throws Exception {

        var currentReqs = getVertexResources(client, jobId);
        if (currentReqs.isEmpty()) {
            System.out.println("No vertex resource requirements found, skipping.");
            return;
        }

        var entries = new ArrayList<>(currentReqs.entrySet());

        boolean allSame =
                entries.stream()
                                .map(e -> e.getValue().getParallelism().getUpperBound())
                                .distinct()
                                .count()
                        == 1;

        var newReqs = new HashMap<>(currentReqs);

        if (allSame) {
            for (int i = 0; i < entries.size(); i++) {
                var entry = entries.get(i);
                int current = entry.getValue().getParallelism().getUpperBound();
                int delta = ((i + roundRobinIndex) % 2 == 0) ? 1 : -1;
                int newParallelism = Math.max(1, current + delta);
                newReqs.put(
                        entry.getKey(),
                        new JobVertexResourceRequirements(
                                new JobVertexResourceRequirements.Parallelism(1, newParallelism)));
            }
        } else {
            for (var entry : entries) {
                int current = entry.getValue().getParallelism().getUpperBound();
                newReqs.put(
                        entry.getKey(),
                        new JobVertexResourceRequirements(
                                new JobVertexResourceRequirements.Parallelism(1, current + 1)));
            }
        }

        updateVertexResources(client, jobId, newReqs);

        System.out.println("Rescale applied (allSame=" + allSame + "):");
        for (var entry : newReqs.entrySet()) {
            int old = currentReqs.get(entry.getKey()).getParallelism().getUpperBound();
            int updated = entry.getValue().getParallelism().getUpperBound();
            System.out.println("  vertex " + entry.getKey() + ": " + old + " -> " + updated);
        }
    }

    private static Map<JobVertexID, JobVertexResourceRequirements> getVertexResources(
            RestClusterClient<String> client, JobID jobId) throws Exception {
        var params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        return client.sendRequest(
                        new JobResourceRequirementsHeaders(),
                        params,
                        EmptyRequestBody.getInstance())
                .get(REST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .asJobResourceRequirements()
                .get()
                .getJobVertexParallelisms();
    }

    private static void updateVertexResources(
            RestClusterClient<String> client,
            JobID jobId,
            Map<JobVertexID, JobVertexResourceRequirements> newReqs)
            throws Exception {
        var params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        client.sendRequest(
                        new JobResourcesRequirementsUpdateHeaders(),
                        params,
                        new JobResourceRequirementsBody(new JobResourceRequirements(newReqs)))
                .get(REST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
