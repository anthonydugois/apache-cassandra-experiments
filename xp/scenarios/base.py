import logging
import enoslib as en

from xp.drivers.Cassandra import Cassandra


def run_scenario(hosts: list[en.Host],
                 seeds: list[en.Host],
                 not_seeds: list[en.Host],
                 clients: list[en.Host],
                 cassandra_image: str,
                 cassandra_conf_template: str):
    cassandra = Cassandra(name="cassandra", docker_image=cassandra_image)

    cassandra.set_hosts(hosts)
    cassandra.set_seeds(seeds)

    if len(not_seeds) > 0:
        cassandra.set_not_seeds(not_seeds)

    cassandra.build_file_tree()
    cassandra.create_config(cassandra_conf_template)

    cassandra.deploy_and_start()

    logging.info(cassandra.nodetool("status"))

    # ...

    cassandra.destroy()
