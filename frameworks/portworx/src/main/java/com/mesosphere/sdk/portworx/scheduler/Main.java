package com.mesosphere.sdk.portworx.scheduler;

import com.mesosphere.sdk.config.validate.TaskEnvCannotChange;
import com.mesosphere.sdk.portworx.api.PortworxResource;
import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.SchedulerBuilder;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.SchedulerRunner;
import com.mesosphere.sdk.specification.DefaultServiceSpec;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.specification.yaml.RawServiceSpec;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Portworx service.
 */
public final class Main {

  private Main() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected one file argument, got: "
       + Arrays.toString(args));
    }
    SchedulerRunner
      .fromSchedulerBuilder(createSchedulerBuilder(new File(args[0])))
      .run();
  }

  private static SchedulerBuilder createSchedulerBuilder(File yamlSpecFile) throws Exception {
    SchedulerConfig schedulerConfig = SchedulerConfig.fromEnv();
    RawServiceSpec rawServiceSpec = RawServiceSpec.newBuilder(yamlSpecFile).build();
    String node = "node";
    String etcdFlag = "ETCD_ENABLED";
    String lighthouse = "lighthouse";
    String state = "start";
    SchedulerBuilder schedulerBuilder = DefaultScheduler.newBuilder(
        DefaultServiceSpec.newGenerator(rawServiceSpec,
        schedulerConfig, yamlSpecFile.getParentFile()).build(),
        schedulerConfig)
        .setCustomConfigValidators(Arrays.asList(
            new TaskEnvCannotChange("etcd-cluster", node, etcdFlag),
            new TaskEnvCannotChange("etcd-proxy", node, etcdFlag),
            new TaskEnvCannotChange(lighthouse, state, "LIGHTHOUSE_ENABLED"),
            new TaskEnvCannotChange(lighthouse, state, "LIGHTHOUSE_ADMIN_USER")))
        .setPlansFrom(rawServiceSpec);

    schedulerBuilder.setCustomResources(getResources(schedulerBuilder.getServiceSpec()));
    return schedulerBuilder;
  }

  private static Collection<Object> getResources(ServiceSpec serviceSpec) {
    final Collection<Object> apiResources = new ArrayList<>();
    apiResources.add(new PortworxResource(serviceSpec));
    return apiResources;
  }
}
