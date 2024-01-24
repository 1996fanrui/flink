/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.ListUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Tracks resource for a single job. */
class JobScopedResourceTracker {

    private static final Logger LOG = LoggerFactory.getLogger(JobScopedResourceTracker.class);

    // only for logging purposes
    private final JobID jobId;

    private final BiDirectionalResourceToRequirementMapping resourceToRequirementMapping =
            new BiDirectionalResourceToRequirementMapping();

    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    private ResourceCounter resourceRequirements = ResourceCounter.empty();
    private ResourceCounter excessResources = ResourceCounter.empty();

    JobScopedResourceTracker(JobID jobId) {
        this.jobId = Preconditions.checkNotNull(jobId);
    }

    public void notifyResourceRequirements(
            Collection<ResourceRequirement> newResourceRequirements) {
        Preconditions.checkNotNull(newResourceRequirements);

        resourceRequirements = ResourceCounter.empty();
        for (ResourceRequirement newResourceRequirement : newResourceRequirements) {
            resourceRequirements =
                    resourceRequirements.add(
                            newResourceRequirement.getResourceProfile(),
                            newResourceRequirement.getNumberOfRequiredSlots(),
                            newResourceRequirement.getCopiedLoadingWeights());
        }
        findExcessSlots();
        tryAssigningExcessSlots();
    }

    public void notifyAcquiredResource(
            ResourceProfile resourceProfile, LoadingWeight loadingWeight) {
        Preconditions.checkNotNull(resourceProfile);
        final Optional<ResourceProfile> matchingRequirement =
                findMatchingRequirement(resourceProfile);
        if (matchingRequirement.isPresent()) {
            resourceToRequirementMapping.incrementCount(
                    matchingRequirement.get(),
                    resourceProfile,
                    1,
                    Lists.newArrayList(loadingWeight));
        } else {
            LOG.debug("Job {} acquired excess resource {}.", resourceProfile, jobId);
            excessResources =
                    excessResources.add(resourceProfile, 1, Lists.newArrayList(loadingWeight));
        }
    }

    private Optional<ResourceProfile> findMatchingRequirement(ResourceProfile resourceProfile) {
        return requirementMatcher.match(
                resourceProfile,
                resourceRequirements,
                resourceToRequirementMapping::getNumFulfillingResources);
    }

    public void notifyLostResource(ResourceProfile resourceProfile, LoadingWeight loadingWeight) {
        Preconditions.checkNotNull(resourceProfile);
        if (excessResources.getResourceCount(resourceProfile) > 0) {
            LOG.trace("Job {} lost excess resource {}.", jobId, resourceProfile);
            excessResources =
                    excessResources.subtract(resourceProfile, 1, Lists.newArrayList(loadingWeight));
            return;
        }

        Set<ResourceProfile> fulfilledRequirements =
                resourceToRequirementMapping
                        .getRequirementsFulfilledBy(resourceProfile)
                        .getResources();
        LOG.debug(
                "notifyLostResource resourceToRequirementMapping {}", resourceToRequirementMapping);
        if (!fulfilledRequirements.isEmpty()) {
            // deduct the resource from any requirement
            // from a correctness standpoint the choice is arbitrary
            // from a resource utilization standpoint it could make sense to search for the
            // requirement with the largest
            // distance to the resource profile (i.e., the smallest requirement), but it may not
            // matter since we are
            // likely to get back a similarly-sized resource later on
            ResourceProfile assignedRequirement = fulfilledRequirements.iterator().next();

            resourceToRequirementMapping.decrementCount(
                    assignedRequirement, resourceProfile, 1, Lists.newArrayList(loadingWeight));

            tryAssigningExcessSlots();
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Job %s lost a resource %s but no such resource was tracked.",
                            jobId, resourceProfile));
        }
    }

    public Collection<ResourceRequirement> getMissingResources() {
        final Collection<ResourceRequirement> missingResources = new ArrayList<>();
        LOG.debug("getMissingResources, resourceRequirements: {}", resourceRequirements);
        for (Map.Entry<ResourceProfile, Integer> requirement :
                resourceRequirements.getResourcesWithCount()) {
            ResourceProfile requirementProfile = requirement.getKey();

            int numRequiredResources = requirement.getValue();
            int numAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);
            List<LoadingWeight> loadingWeightsOfRequirement =
                    resourceRequirements.getLoadingWeights(requirementProfile);

            ListUtil.remove(
                    loadingWeightsOfRequirement,
                    resourceToRequirementMapping.getNumFulfillingResourceLoadingsOf(
                            requirementProfile));

            LOG.debug(
                    "getMissingResources resourceToRequirementMapping {}",
                    resourceToRequirementMapping);
            if (numAcquiredResources < numRequiredResources) {
                missingResources.add(
                        ResourceRequirement.create(
                                requirementProfile,
                                numRequiredResources - numAcquiredResources,
                                loadingWeightsOfRequirement));
            }
        }
        return missingResources;
    }

    public Collection<ResourceRequirement> getAcquiredResources() {
        final Set<ResourceProfile> knownResourceProfiles = new HashSet<>();
        knownResourceProfiles.addAll(resourceToRequirementMapping.getAllResourceProfiles());
        knownResourceProfiles.addAll(excessResources.getResources());

        final ResourceCounter resourceCounter =
                resourceToRequirementMapping.getAllResourceCounter().add(excessResources);

        final List<ResourceRequirement> acquiredResources = new ArrayList<>();
        for (ResourceProfile knownResourceProfile : knownResourceProfiles) {
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfilledRequirements(knownResourceProfile)
                            + excessResources.getResourceCount(knownResourceProfile);
            ResourceRequirement resourceRequirement =
                    ResourceRequirement.create(
                            knownResourceProfile,
                            numTotalAcquiredResources,
                            resourceCounter.getLoadingWeights(knownResourceProfile));
            acquiredResources.add(resourceRequirement);
        }

        return acquiredResources;
    }

    public boolean isEmpty() {
        return resourceRequirements.isEmpty() && excessResources.isEmpty();
    }

    public boolean isRequirementEmpty() {
        return resourceRequirements.isEmpty();
    }

    private void findExcessSlots() {
        final Collection<ExcessResource> excessResources = new ArrayList<>();

        for (ResourceProfile requirementProfile :
                resourceToRequirementMapping.getAllRequirementProfiles()) {
            int numTotalRequiredResources =
                    resourceRequirements.getResourceCount(requirementProfile);
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);

            if (numTotalAcquiredResources > numTotalRequiredResources) {
                int numExcessResources = numTotalAcquiredResources - numTotalRequiredResources;
                ResourceCounter excessResourceCounter =
                        resourceToRequirementMapping
                                .getAllResourceCounter()
                                .subtract(resourceRequirements);

                List<LoadingWeight> loadingWeightsOfFulfillingResource =
                        excessResourceCounter.getLoadingWeights(requirementProfile);
                for (Map.Entry<ResourceProfile, Integer> acquiredResource :
                        resourceToRequirementMapping
                                .getResourcesFulfilling(requirementProfile)
                                .getResourcesWithCount()) {
                    ResourceProfile acquiredResourceProfile = acquiredResource.getKey();
                    int numAcquiredResources = acquiredResource.getValue();
                    List<LoadingWeight> loadingWeightsOfAcquired =
                            new ArrayList<>(
                                    resourceToRequirementMapping.getNumFulfillingResourceLoadingsOf(
                                            acquiredResource.getKey()));

                    if (numAcquiredResources <= numExcessResources) {
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numAcquiredResources,
                                        loadingWeightsOfAcquired));

                        numExcessResources -= numAcquiredResources;
                        ListUtil.remove(
                                loadingWeightsOfFulfillingResource, loadingWeightsOfAcquired);
                    } else {
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numExcessResources,
                                        loadingWeightsOfFulfillingResource));
                        break;
                    }
                }
            }
        }

        if (!excessResources.isEmpty()) {
            LOG.debug("Detected excess resources for job {}: {}", jobId, excessResources);
            for (ExcessResource excessResource : excessResources) {
                resourceToRequirementMapping.decrementCount(
                        excessResource.requirementProfile,
                        excessResource.resourceProfile,
                        excessResource.numExcessResources,
                        excessResource.loadingWeights);
                this.excessResources =
                        this.excessResources.add(
                                excessResource.resourceProfile,
                                excessResource.numExcessResources,
                                excessResource.loadingWeights);
            }
        }
    }

    private void tryAssigningExcessSlots() {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} before re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }

        ResourceCounter assignedResources = ResourceCounter.empty();
        for (Map.Entry<ResourceProfile, Integer> excessResource :
                excessResources.getResourcesWithCount()) {
            List<LoadingWeight> loadingWeights =
                    excessResources.getLoadingWeights(excessResource.getKey());
            for (int i = 0; i < excessResource.getValue(); i++) {
                final ResourceProfile resourceProfile = excessResource.getKey();
                final Optional<ResourceProfile> matchingRequirement =
                        findMatchingRequirement(resourceProfile);
                if (matchingRequirement.isPresent()) {
                    resourceToRequirementMapping.incrementCount(
                            matchingRequirement.get(),
                            resourceProfile,
                            1,
                            Lists.newArrayList(loadingWeights.get(i)));
                    assignedResources =
                            assignedResources.add(
                                    resourceProfile, 1, Lists.newArrayList(loadingWeights.get(i)));
                } else {
                    break;
                }
            }
        }

        for (Map.Entry<ResourceProfile, Integer> assignedResource :
                assignedResources.getResourcesWithCount()) {
            List<LoadingWeight> loadingWeights =
                    assignedResources.getLoadingWeights(assignedResource.getKey());
            excessResources =
                    excessResources.subtract(
                            assignedResource.getKey(), assignedResource.getValue(), loadingWeights);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} after re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }
    }

    @Override
    public String toString() {
        return "JobScopedResourceTracker{"
                + "jobId="
                + jobId
                + ", resourceToRequirementMapping="
                + resourceToRequirementMapping
                + ", requirementMatcher="
                + requirementMatcher
                + ", resourceRequirements="
                + resourceRequirements
                + ", excessResources="
                + excessResources
                + '}';
    }

    private static class ExcessResource {
        private final ResourceProfile requirementProfile;
        private final ResourceProfile resourceProfile;
        private final int numExcessResources;
        private final List<LoadingWeight> loadingWeights;

        private ExcessResource(
                ResourceProfile requirementProfile,
                ResourceProfile resourceProfile,
                int numExcessResources,
                List<LoadingWeight> loadingWeights) {
            this.requirementProfile = requirementProfile;
            this.resourceProfile = resourceProfile;
            this.numExcessResources = numExcessResources;
            this.loadingWeights = loadingWeights;
        }

        @Override
        public String toString() {
            return "ExcessResource{"
                    + "numExcessResources="
                    + numExcessResources
                    + ", requirementProfile="
                    + requirementProfile
                    + ", resourceProfile="
                    + resourceProfile
                    + ", loadingWeight="
                    + loadingWeights
                    + '}';
        }
    }
}
