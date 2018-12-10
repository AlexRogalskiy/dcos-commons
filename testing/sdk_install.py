"""Utilities relating to installing services

************************************************************************
FOR THE TIME BEING WHATEVER MODIFICATIONS ARE APPLIED TO THIS FILE
SHOULD ALSO BE APPLIED TO sdk_install IN ANY OTHER PARTNER REPOS
************************************************************************
"""
import json
import logging
import time
import retrying
import tempfile
from enum import Enum
from typing import Any, Dict, Optional, Set, Union

import sdk_cmd
import sdk_marathon
import sdk_plan
import sdk_tasks
import sdk_utils
from dcos import (marathon, mesos)
from time import sleep
import json

log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 15 * 60

"""List of services which are currently installed via install().
Used by post - test diagnostics to retrieve stuff from currently running services."""
_installed_service_names: Set[str] = set([])

"""List of dead agents which should be ignored when checking for orphaned resources.
Used by uninstall when validating that an uninstall completed successfully."""
_dead_agent_hosts: Set[str] = set([])


def get_installed_service_names() -> Set[str]:
    """Returns the a set of service names which had been installed via sdk_install in this session."""
    return _installed_service_names


class PackageVersion(Enum):
    STUB_UNIVERSE = "stub-universe"
    LATEST_UNIVERSE = ""


@retrying.retry(stop_max_attempt_number=3, retry_on_exception=lambda e: isinstance(e, Exception))
def _retried_install_impl(
    package_name: str,
    service_name: str,
    expected_running_tasks: int,
    package_version: Optional[str],
    options: Dict[str, Any],
    timeout_seconds: int,
    wait_for_all_conditions: bool,
) -> None:
    log.info(
        "Installing package={} service={} with options={} version={}".format(
            package_name, service_name, options, package_version
        )
    )

    # Trigger package install, but only if it's not already installed.
    # We expect upstream to have confirmed that it wasn't already installed beforehand.
    install_cmd = ["package", "install", package_name, "--yes"]

    if package_version:
        install_cmd.append("--package-version={}".format(package_version))

    if sdk_marathon.app_exists(service_name):
        log.info(
            "Marathon app={} exists, ensuring CLI for package={} is installed".format(
                service_name, package_name
            )
        )
        install_cmd.append("--cli")
    elif options:
        # Write options to a temporary json file to be accessed by the CLI:
        options_file = tempfile.NamedTemporaryFile("w")
        json.dump(options, options_file)
        options_file.flush()  # ensure content is available for the CLI to read below
        install_cmd.append("--options={}".format(options_file.name))

    sdk_cmd.run_cli(" ".join(install_cmd), check=True)

    # Wait for expected tasks to come up
    if expected_running_tasks > 0 and wait_for_all_conditions:
        sdk_tasks.check_running(
            service_name=service_name,
            expected_task_count=expected_running_tasks,
            timeout_seconds=timeout_seconds,
        )

    # Wait for completed marathon deployment
    if wait_for_all_conditions:
        sdk_marathon.wait_for_deployment(service_name, timeout_seconds, None)


def install(
    package_name: str,
    service_name: str,
    expected_running_tasks: int,
    additional_options: Dict[str, Any] = {},
    package_version: Optional[Union[PackageVersion, str]] = PackageVersion.STUB_UNIVERSE,
    timeout_seconds: int = TIMEOUT_SECONDS,
    wait_for_deployment: bool = True,
    insert_strict_options: bool = True,
    wait_for_all_conditions: bool = True,
) -> None:
    start = time.time()

    # If the package is already installed at this point, fail immediately.
    if sdk_marathon.app_exists(service_name):
        raise Exception("Service is already installed: {}".format(service_name))

    if insert_strict_options and sdk_utils.is_strict_mode():
        # strict mode requires correct principal and secret to perform install.
        # see also: sdk_security.py
        options = sdk_utils.merge_dictionaries(
            {
                "service": {
                    "service_account": "service-acct",
                    "principal": "service-acct",
                    "service_account_secret": "secret",
                    "secret_name": "secret",
                }
            },
            additional_options,
        )
    else:
        options = additional_options

    options = sdk_utils.merge_dictionaries(
        {
            "service": {
                "name": service_name
            },
        },
        options,
    )

    # 1. Install package, wait for tasks, wait for marathon deployment
    _retried_install_impl(
        package_name,
        service_name,
        expected_running_tasks,
        package_version.value if isinstance(package_version, PackageVersion) else package_version,
        options,
        timeout_seconds,
        wait_for_all_conditions
    )

    # 2. Wait for the scheduler to be idle (as implied by deploy plan completion and suppressed bit)
    # This should be skipped ONLY when it's known that the scheduler will be stuck in an incomplete
    # state, or if the thing being installed doesn't have a deployment plan (e.g. standalone app)
    if wait_for_deployment:
        # this can take a while, default is 15 minutes. for example with HDFS, we can hit the expected
        # total task count via FINISHED tasks, without actually completing deployment
        log.info(
            "Waiting for package={} service={} to finish deployment plan...".format(
                package_name, service_name
            )
        )
        sdk_plan.wait_for_completed_deployment(
            service_name=service_name,
            timeout_seconds=timeout_seconds,
        )

    log.info(
        "Installed package={} service={} after {}".format(
            package_name, service_name, sdk_utils.pretty_duration(time.time() - start)
        )
    )

    global _installed_service_names
    _installed_service_names.add(service_name)


@retrying.retry(
    stop_max_attempt_number=5,
    wait_fixed=5000,
    retry_on_exception=lambda e: isinstance(e, Exception),
)
def _retried_run_janitor(service_name: str) -> None:
    cmd_list = [
        "sudo",
        "docker",
        "run",
        "mesosphere/janitor",
        "/janitor.py",
        "-r",
        sdk_utils.get_role(service_name),
        "-p",
        service_name + "-principal",
        "-z",
        sdk_utils.get_zk_path(service_name),
        "--auth_token={}".format(sdk_utils.dcos_token()),
    ]
    rc, _, _ = sdk_cmd.master_ssh(" ".join(cmd_list))
    assert rc == 0, "Janitor command failed"


@retrying.retry(
    stop_max_attempt_number=5,
    wait_fixed=5000,
    retry_on_exception=lambda e: isinstance(e, Exception),
)
def _retried_uninstall_package_and_wait(package_name: str, service_name: str) -> None:
    if sdk_marathon.app_exists(service_name):
        log.info("Uninstalling package {} with service name {}".format(package_name, service_name))
        sdk_cmd.run_cli(
            "package uninstall {} --app-id={} --yes".format(package_name, service_name), check=True
        )

        # Wait on the app no longer being listed in Marathon, at which point it is uninstalled.
        # At the same time, log the deploy plan state as we wait for the app to finish uninstalling.
        @retrying.retry(
            stop_max_delay=TIMEOUT_SECONDS * 1000,
            wait_fixed=5000,
            retry_on_result=lambda result: not result,
        )
        def wait_for_removal_log_deploy_plan() -> bool:
            if not sdk_marathon.app_exists(service_name):
                return True

            # App still exists, print the deploy plan. Best effort: It is expected for the scheduler
            # to become unavailable once uninstall completes.
            try:
                log.info(
                    sdk_plan.plan_string("deploy", sdk_plan.get_plan_once(service_name, "deploy"))
                )
            except Exception:
                pass  # best effort attempt at logging plan content
            return False

        log.info("Waiting for {} to be removed".format(service_name))
        wait_for_removal_log_deploy_plan()
    else:
        log.info(
            'Skipping uninstall of package {}/service {}: App named "{}" doesn\'t exist'.format(
                package_name, service_name, service_name
            )
        )


def _verify_completed_uninstall(service_name: str) -> None:
    state_summary = sdk_cmd.cluster_request("GET", "/mesos/state-summary").json()

    # There should be no orphaned resources in the state summary (DCOS-30314)
    orphaned_resources = 0
    ignored_orphaned_resources = 0
    service_role = sdk_utils.get_role(service_name)
    for agent in state_summary["slaves"]:
        # resources should be grouped by role. check for any resources in our expected role:
        matching_reserved_resources = agent["reserved_resources"].get(service_role)
        if matching_reserved_resources:
            global _dead_agent_hosts
            if agent["hostname"] in _dead_agent_hosts:
                # The test told us ahead of time to expect orphaned resources on this host.
                log.info(
                    "Ignoring orphaned resources on agent {}/{}: {}".format(
                        agent["id"], agent["hostname"], matching_reserved_resources
                    )
                )
                ignored_orphaned_resources += len(matching_reserved_resources)
            else:
                log.error(
                    "Orphaned resources on agent {}/{}: {}".format(
                        agent["id"], agent["hostname"], matching_reserved_resources
                    )
                )
                orphaned_resources += len(matching_reserved_resources)
    if orphaned_resources:
        log.error(
            "{} orphaned resources (plus {} ignored) after uninstall of {}".format(
                orphaned_resources, ignored_orphaned_resources, service_name
            )
        )
        log.error(state_summary)
        raise Exception(
            "Found {} orphaned resources (plus {} ignored) after uninstall of {}".format(
                orphaned_resources, ignored_orphaned_resources, service_name
            )
        )
    elif ignored_orphaned_resources:
        log.info(
            "Ignoring {} orphaned resources after uninstall of {}".format(
                ignored_orphaned_resources, service_name
            )
        )
        log.info(state_summary)
    else:
        log.info("No orphaned resources for role {} were found".format(service_role))

    # There should be no framework entry for this service in the state summary (DCOS-29474)
    orphaned_frameworks = [
        fwk for fwk in state_summary["frameworks"] if fwk["name"] == service_name
    ]
    if orphaned_frameworks:
        log.error(
            "{} orphaned frameworks named {} after uninstall of {}: {}".format(
                len(orphaned_frameworks), service_name, service_name, orphaned_frameworks
            )
        )
        log.error(state_summary)
        raise Exception(
            "Found {} orphaned frameworks named {} after uninstall of {}: {}".format(
                len(orphaned_frameworks), service_name, service_name, orphaned_frameworks
            )
        )
    log.info("No orphaned frameworks for service {} were found".format(service_name))


def ignore_dead_agent(agent_host: str) -> None:
    """Marks the specified agent as destroyed. When uninstall() is next called, any orphaned
    resources against this agent will be logged but will not result in a thrown exception.
    """
    global _dead_agent_hosts
    _dead_agent_hosts.add(agent_host)
    log.info(
        "Added {} to expected dead agents for resource validation purposes: {}".format(
            agent_host, _dead_agent_hosts
        )
    )


def uninstall(package_name: str, service_name: str) -> None:
    """Uninstalls the specified service from the cluster, and verifies that its resources and
    framework were correctly cleaned up after the uninstall has completed. Any agents which are
    expected to have orphaned resources (e.g. due to being shut down) should be passed to
    ignore_dead_agent() before triggering the uninstall.
    """
    start = time.time()

# Portworx volume specific cleanups are done here. 
def portworx_vol_cleanup():
    client = mesos.DCOSClient()
    agents = client.get_state_summary()['slaves']
    exit_status = True
    output_agent = ""
    # The framework tests only unmount and detach the portworx volumes created during tests
    # Find the portworx volumes and delete those.
    log.info("PORTWORX: cleanup portworx volumes")
    try:
        exit_status, output_agent = shakedown.run_command_on_agent(agents[0]['hostname'],
            'pxctl -j v l', 'vagrant','/ssh/key')
    except:
        log.info("PORTWORX: Skipping portworx specific cleanups")
        return 1

    if exit_status != True:
        log.info("PORTWORX: Failed to collect px-volume list {}".format(output_agent))
        return 1 
    
    pxvols = json.loads(output_agent)
    sleep(5) # Extra time after detach volumes before deleting. 
    for vol in pxvols:
        log.info("Deleting Portworx Volume: {}".format(vol['locator']['name']))
        cmd = 'pxctl v d -f ' + vol['locator']['name'] 
        exit_status, output_agent = shakedown.run_command_on_agent(agents[0]['hostname'], cmd, 'vagrant','/ssh/key')	
        if exit_status != True:
            log.info("PORTWORX: Failed to delete px-volume {} {}".format(vol['locator']['name'], output_agent))
    return 0

# Portworx cleanups on every node
def portworx_cleanup():
    client = mesos.DCOSClient()
    agents = client.get_state_summary()['slaves']
    exit_status = True
    output_agent = ""

    for i in range(len(agents)):
        log.info("PORTWORX: i==> {} Agent hostname: {}".format(i,  agents[i]['hostname']))
        # Skipping exit status checks
        cmd = 'sudo systemctl stop portworx'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo docker rm portworx.service -f'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo rm -f /etc/systemd/system/portworx.service'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo rm -f /etc/systemd/system/dcos.target.wants/portworx.service'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo rm -f /etc/systemd/system/multi-user.target.wants/portworx.service'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo systemctl daemon-reload'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo /opt/pwx/bin/pxctl service node-wipe --all'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo chattr -i /etc/pwx/.private.json'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo rm -rf /etc/pwx'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo umount /opt/pwx/oci'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo rm -rf /opt/pwx'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        cmd = 'sudo rmmod px -f'
        exit_status, output_agent = shakedown.run_command_on_agent(agents[i]['hostname'], cmd, 'vagrant','/ssh/key')
        # Skipping exit status checks
    return 0

    # Display current SDK Plan before uninstall, helps with debugging stuck uninstalls
    log.info("Current plan status for {}".format(service_name))
    sdk_cmd.svc_cli(package_name, service_name, "plan status deploy", check=False)

    try:
        _retried_uninstall_package_and_wait(package_name, service_name)
    except Exception:
        log.exception("Got exception when uninstalling {}".format(service_name))
        raise

    cleanup_start = time.time()

    try:
        if sdk_utils.dcos_version_less_than("1.10"):
            # 1.9 and earlier: Run janitor to unreserve resources
            log.info("Janitoring {}".format(service_name))
            _retried_run_janitor(service_name)
    except Exception:
        log.exception("Got exception when cleaning up {}".format(service_name))
        raise

    finish = time.time()

    log.info(
        "Uninstalled {} after pkg({}) + cleanup({}) = total({})".format(
            service_name,
            sdk_utils.pretty_duration(cleanup_start - start),
            sdk_utils.pretty_duration(finish - cleanup_start),
            sdk_utils.pretty_duration(finish - start),
        )
    )

    # Sanity check: Verify that all resources and the framework have been successfully cleaned up,
    # and throw an exception if anything is left over (uninstall bug?)
    _verify_completed_uninstall(service_name)

    # Finally, remove the service from the installed list (used by sdk_diag)
    global _installed_service_names
    try:
        _installed_service_names.remove(service_name)
    except KeyError:
        pass # allow tests to 'uninstall' up-front

    if sdk_utils.dcos_version_less_than('1.10'):
        log.info('Uninstalling/janitoring {}'.format(service_name))
        try:
            shakedown.uninstall_package_and_wait(
                package_name, service_name=service_name)
        except (dcos.errors.DCOSException, ValueError) as e:
            log.info('Got exception when uninstalling package, ' +
                          'continuing with janitor anyway: {}'.format(e))
            if 'marathon' in str(e):
                log.info('Detected a probable marathon flake. Raising so retry will trigger.')
                raise

        janitor_start = time.time()

        # leading slash removed, other slashes converted to double underscores:
        deslashed_service_name = service_name.lstrip('/').replace('/', '__')
        if role is None:
            role = deslashed_service_name + '-role'
        if service_account is None:
            service_account = service_name + '-principal'
        if zk is None:
            zk = 'dcos-service-' + deslashed_service_name
        janitor_cmd = ('docker run mesosphere/janitor /janitor.py '
                       '-r {role} -p {service_account} -z {zk} --auth_token={auth}')
        shakedown.run_command_on_master(
            janitor_cmd.format(
                role=role,
                service_account=service_account,
                zk=zk,
                auth=sdk_cmd.run_cli('config show core.dcos_acs_token', print_output=False).strip()))

        finish = time.time()

        log.info(
            'Uninstall done after pkg({}) + janitor({}) = total({})'.format(
                shakedown.pretty_duration(janitor_start - start),
                shakedown.pretty_duration(finish - janitor_start),
                shakedown.pretty_duration(finish - start)))
    else:
        log.info('Uninstalling {}'.format(service_name))
        try:
            shakedown.uninstall_package_and_wait(
                package_name, service_name=service_name)
            # service_name may already contain a leading slash:
            marathon_app_id = '/' + service_name.lstrip('/')
            log.info('Waiting for no deployments for {}'.format(marathon_app_id))
            shakedown.deployment_wait(TIMEOUT_SECONDS, marathon_app_id)

            # wait for service to be gone according to marathon
            client = shakedown.marathon.create_client()
            def marathon_dropped_service():
                app_ids = [app['id'] for app in client.get_apps()]
                log.info('Marathon apps: {}'.format(app_ids))
                matching_app_ids = [
                    app_id for app_id in app_ids if app_id == marathon_app_id
                ]
                if len(matching_app_ids) > 1:
                    log.warning('Found multiple apps with id {}'.format(
                        marathon_app_id))
                return len(matching_app_ids) == 0
            log.info('Waiting for no {} Marathon app'.format(marathon_app_id))
            shakedown.time_wait(marathon_dropped_service, timeout_seconds=TIMEOUT_SECONDS)

        except (dcos.errors.DCOSException, ValueError) as e:
            log.info(
                'Got exception when uninstalling package: {}'.format(e))
            if 'marathon' in str(e):
                log.info('Detected a probable marathon flake. Raising so retry will trigger.')
                raise
        finally:
            sdk_utils.list_reserved_resources()
    # Call portworx specific cleanup routine at the end.
    _portworx_volcleanup()

def merge_dictionaries(dict1, dict2):
    if (not isinstance(dict2, dict)):
        return dict1
    ret = {}
    for k, v in dict1.items():
        ret[k] = v
    for k, v in dict2.items():
        if (k in dict1 and isinstance(dict1[k], dict)
                and isinstance(dict2[k], collections.Mapping)):
            ret[k] = merge_dictionaries(dict1[k], dict2[k])
        else:
            ret[k] = dict2[k]
    return ret
